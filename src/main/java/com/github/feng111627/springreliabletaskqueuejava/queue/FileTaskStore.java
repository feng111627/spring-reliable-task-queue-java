package com.github.feng111627.springreliabletaskqueuejava.queue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于本地 JSON 文件的任务存储，模拟队列后端：
 * - 全量记录保存在内存 Map，每次变更后整体落盘（写临时文件 + 原子 move）；
 * - 读写锁保证进程内一致性；version 乐观锁保证领取/完成的并发安全；
 * - 重启时从文件恢复，未完成任务不丢、已完成任务不重复执行。
 */
@Component
public class FileTaskStore implements TaskStore {

    private static final Logger log = LoggerFactory.getLogger(FileTaskStore.class);

    private final Path storeFile;
    private final ObjectMapper mapper;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, TaskRecord> byId = new LinkedHashMap<>();
    /** 幂等键 = 业务键 + 负载：同一任务的重复提交（如客户端重试）命中去重 */
    private final Map<String, String> idByDedupKey = new HashMap<>();

    public FileTaskStore(QueueProperties properties) {
        this.storeFile = Path.of(properties.getStorageDir()).resolve("tasks.json");
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @PostConstruct
    void load() {
        lock.writeLock().lock();
        try {
            if (Files.exists(storeFile)) {
                List<TaskRecord> records = mapper.readValue(storeFile.toFile(), new TypeReference<>() {
                });
                for (TaskRecord r : records) {
                    byId.put(r.getId(), r);
                    idByDedupKey.put(dedupKey(r.getBusinessKey(), r.getPayload()), r.getId());
                }
                // 兼容旧版本数据：旧记录没有 seq/priority/notBefore，反序列化得到默认值 0/null，
                // 同键旧记录 seq 均为 0，领取时按“seq>0 才算前辈”处理，退化为按创建时间保序（旧行为）。
                log.info("[store] 从 {} 恢复 {} 条任务记录", storeFile, records.size());
            } else {
                log.info("[store] 存储文件 {} 不存在，从空队列启动", storeFile);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("加载任务存储失败: " + storeFile, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void persistLocked() {
        try {
            Files.createDirectories(storeFile.getParent());
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), new ArrayList<>(byId.values()));
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化任务存储失败: " + storeFile, e);
        }
    }

    static String dedupKey(String businessKey, String payload) {
        return businessKey + "\u0000" + (payload == null ? "" : payload);
    }

    @Override
    public SubmitResult insertIfAbsent(TaskRecord task) {
        lock.writeLock().lock();
        try {
            String existingId = idByDedupKey.get(dedupKey(task.getBusinessKey(), task.getPayload()));
            if (existingId != null) {
                TaskRecord existing = byId.get(existingId);
                return SubmitResult.duplicate(existing.getId(), existing.getStatus());
            }
            byId.put(task.getId(), task);
            // 同业务键内分配单调递增序号：优先级只在“不同键/同键队头之间”生效，不能打乱同键先后。
            task.setSeq(nextSeqLocked(task.getBusinessKey()));
            idByDedupKey.put(dedupKey(task.getBusinessKey(), task.getPayload()), task.getId());
            persistLocked();
            return SubmitResult.accepted(task.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private long nextSeqLocked(String businessKey) {
        long max = 0;
        for (TaskRecord t : byId.values()) {
            if (t.getBusinessKey().equals(businessKey) && t.getSeq() > max) {
                max = t.getSeq();
            }
        }
        return max + 1;
    }

    /** 任务是否仍是活动态（占着同键队头位置）：排队、执行中、退避重试中都算；终态不算。 */
    private static boolean active(TaskStatus s) {
        return s == TaskStatus.PENDING || s == TaskStatus.RUNNING || s == TaskStatus.FAILED;
    }

    @Override
    public Optional<TaskRecord> findByBusinessKey(String businessKey) {
        lock.readLock().lock();
        try {
            // 同一业务键可能有多条任务（不同负载），返回最新创建的一条
            return byId.values().stream()
                    .filter(t -> t.getBusinessKey().equals(businessKey))
                    .max(Comparator.comparing(TaskRecord::getCreatedAt))
                    .map(TaskRecord::copy);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<TaskRecord> findById(String id) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(byId.get(id)).map(TaskRecord::copy);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<TaskRecord> claimDue(Instant now, int max) {
        lock.writeLock().lock();
        try {
            // 候选：PENDING/FAILED 且到了可领取时间（首次延迟 notBefore/退避后的 nextAttemptAt 都体现在此字段）
            List<TaskRecord> candidates = byId.values().stream()
                    .filter(t -> (t.getStatus() == TaskStatus.PENDING || t.getStatus() == TaskStatus.FAILED)
                            && !t.getNextAttemptAt().isAfter(now))
                    .sorted(Comparator.comparingInt(TaskRecord::getPriority).reversed()
                            .thenComparing(TaskRecord::getCreatedAt))
                    .toList();

            List<TaskRecord> claimed = new ArrayList<>();
            // 按优先级顺序领取，但同一业务键必须严格先来后到：
            // 该键还有序号更小的活动任务（未到点/执行中/退避中）时，后面的任务一律不能提前挑走。
            // 注意：不能因为某个后辈被挡就屏蔽整个键——真正的同键队头可能就排在它后面，
            // 必须对每个候选独立判定。
            for (TaskRecord t : candidates) {
                if (claimed.size() >= max) {
                    break;
                }
                if (hasEarlierActiveSiblingLocked(t)) {
                    continue;
                }
                t.setStatus(TaskStatus.RUNNING);
                t.setVersion(t.getVersion() + 1);
                t.setUpdatedAt(Instant.now());
                claimed.add(t.copy());
            }
            if (!claimed.isEmpty()) {
                persistLocked();
            }
            return claimed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 是否存在同业务键、序号更小且仍处于活动态（排队/执行中/退避中）的前辈任务。 */
    private boolean hasEarlierActiveSiblingLocked(TaskRecord t) {
        for (TaskRecord other : byId.values()) {
            if (other != t
                    && other.getBusinessKey().equals(t.getBusinessKey())
                    && other.getSeq() > 0 && other.getSeq() < t.getSeq()
                    && active(other.getStatus())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean markSucceeded(String id, long version) {
        lock.writeLock().lock();
        try {
            TaskRecord t = byId.get(id);
            if (t == null || t.getStatus() != TaskStatus.RUNNING || t.getVersion() != version) {
                return false;
            }
            t.setStatus(TaskStatus.SUCCEEDED);
            t.setLastError(null);
            t.setVersion(version + 1);
            t.setUpdatedAt(Instant.now());
            persistLocked();
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean markFailed(String id, long version, String error, Instant nextAttemptAt, boolean exhausted) {
        lock.writeLock().lock();
        try {
            TaskRecord t = byId.get(id);
            if (t == null || t.getStatus() != TaskStatus.RUNNING || t.getVersion() != version) {
                return false;
            }
            t.setAttempts(t.getAttempts() + 1);
            t.setLastError(error);
            t.setVersion(version + 1);
            t.setUpdatedAt(Instant.now());
            if (exhausted) {
                t.setStatus(TaskStatus.DEAD);
            } else {
                t.setStatus(TaskStatus.FAILED);
                t.setNextAttemptAt(nextAttemptAt);
            }
            persistLocked();
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int recoverRunning() {
        lock.writeLock().lock();
        try {
            int recovered = 0;
            for (TaskRecord t : byId.values()) {
                if (t.getStatus() == TaskStatus.RUNNING) {
                    t.setStatus(TaskStatus.PENDING);
                    t.setNextAttemptAt(Instant.now());
                    t.setVersion(t.getVersion() + 1);
                    t.setUpdatedAt(Instant.now());
                    recovered++;
                }
            }
            if (recovered > 0) {
                persistLocked();
                log.warn("[store] 重启恢复：回收 {} 个上次进程遗留的 RUNNING 任务为 PENDING", recovered);
            }
            return recovered;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean requeueDead(String id) {
        lock.writeLock().lock();
        try {
            TaskRecord t = byId.get(id);
            if (t == null || t.getStatus() != TaskStatus.DEAD) {
                return false;
            }
            t.setStatus(TaskStatus.PENDING);
            t.setAttempts(0);
            t.setLastError(null);
            t.setNextAttemptAt(Instant.now());
            t.setVersion(t.getVersion() + 1);
            t.setUpdatedAt(Instant.now());
            persistLocked();
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public CancelResult cancel(String id) {
        lock.writeLock().lock();
        try {
            TaskRecord t = byId.get(id);
            if (t == null) {
                return CancelResult.notFound(id);
            }
            switch (t.getStatus()) {
                case CANCELLED -> {
                    return CancelResult.alreadyCancelled(id);
                }
                case SUCCEEDED -> {
                    return CancelResult.alreadySucceeded(id);
                }
                case RUNNING -> {
                    return CancelResult.running(id);
                }
                case PENDING, FAILED, DEAD -> {
                    t.setStatus(TaskStatus.CANCELLED);
                    t.setVersion(t.getVersion() + 1);
                    t.setUpdatedAt(Instant.now());
                    // 释放幂等键：取消后允许用同一业务键+同一负载重新提交新任务；
                    // 仅当映射仍指向本任务时才移除，避免误删重新提交后建立的新映射。
                    String dedup = dedupKey(t.getBusinessKey(), t.getPayload());
                    if (id.equals(idByDedupKey.get(dedup))) {
                        idByDedupKey.remove(dedup);
                    }
                    persistLocked();
                    log.info("[store] 任务已取消 taskId={} businessKey={}", id, t.getBusinessKey());
                    return CancelResult.cancelled(id);
                }
                default -> {
                    return CancelResult.notFound(id);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<TaskRecord> listByStatus(TaskStatus status) {
        lock.readLock().lock();
        try {
            return byId.values().stream()
                    .filter(t -> t.getStatus() == status)
                    .map(TaskRecord::copy)
                    .toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<TaskRecord> listAll() {
        lock.readLock().lock();
        try {
            return byId.values().stream().map(TaskRecord::copy).toList();
        } finally {
            lock.readLock().unlock();
        }
    }
}
