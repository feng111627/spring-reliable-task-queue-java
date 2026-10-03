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

    /**
     * 测试辅助：把调用方基于 findById 副本修改后的字段写回并持久化。
     * 生产代码的所有状态变更都应走 claim/mark/cancel 等带状态与版本校验的方法。
     */
    public void updateForTest(TaskRecord updated) {
        lock.writeLock().lock();
        try {
            TaskRecord current = byId.get(updated.getId());
            if (current == null) {
                throw new IllegalArgumentException("任务不存在: " + updated.getId());
            }
            current.setStatus(updated.getStatus());
            current.setNextAttemptAt(updated.getNextAttemptAt());
            current.setNotBefore(updated.getNotBefore());
            current.setAttempts(updated.getAttempts());
            current.setVersion(updated.getVersion());
            current.setUpdatedAt(Instant.now());
            persistLocked();
        } finally {
            lock.writeLock().unlock();
        }
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
            idByDedupKey.put(dedupKey(task.getBusinessKey(), task.getPayload()), task.getId());
            persistLocked();
            return SubmitResult.accepted(task.getId());
        } finally {
            lock.writeLock().unlock();
        }
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
            // 1) 每个业务键只允许“最早创建的非终结任务”参与领取：
            //    更早的任务哪怕还在等定时点/退避，也会挡住后面的任务（严格先来后到，优先级不能越级）
            Map<String, TaskRecord> earliestActiveByKey = new HashMap<>();
            for (TaskRecord t : byId.values()) {
                if (isTerminal(t.getStatus())) {
                    continue;
                }
                TaskRecord cur = earliestActiveByKey.get(t.getBusinessKey());
                if (cur == null || CREATED_ORDER.compare(t, cur) < 0) {
                    earliestActiveByKey.put(t.getBusinessKey(), t);
                }
            }
            // 2) 队首任务还必须到期（含定时投递点 notBefore 与失败退避 nextAttemptAt）
            List<TaskRecord> due = earliestActiveByKey.values().stream()
                    .filter(t -> (t.getStatus() == TaskStatus.PENDING || t.getStatus() == TaskStatus.FAILED)
                            && !t.getNextAttemptAt().isAfter(now))
                    // 3) 积压时高优先级先被消费；同优先级仍按创建时间 FIFO（priority 缺省 0 时与旧行为一致）
                    .sorted(CLAIM_ORDER)
                    .limit(max)
                    .toList();
            List<TaskRecord> claimed = new ArrayList<>(due.size());
            for (TaskRecord t : due) {
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

    /** 终态：不再参与调度，也不再阻塞同业务键的后续任务。 */
    private static boolean isTerminal(TaskStatus status) {
        return status == TaskStatus.SUCCEEDED || status == TaskStatus.DEAD || status == TaskStatus.CANCELLED;
    }

    /** 同键内的先后次序：创建时间为主，id 兜底（同毫秒提交也有确定次序）。 */
    private static final Comparator<TaskRecord> CREATED_ORDER =
            Comparator.comparing(TaskRecord::getCreatedAt).thenComparing(TaskRecord::getId);

    /** 领取次序：优先级降序，再按创建次序 FIFO。 */
    private static final Comparator<TaskRecord> CLAIM_ORDER =
            Comparator.comparingInt(TaskRecord::getPriority).reversed().thenComparing(CREATED_ORDER);

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
    public CancelResult cancel(String id, String reason) {
        lock.writeLock().lock();
        try {
            TaskRecord t = byId.get(id);
            if (t == null) {
                return CancelResult.notFound(id);
            }
            switch (t.getStatus()) {
                case PENDING, FAILED, DEAD -> {
                    // 取消生效：终态化并释放幂等键，之后同一业务键+负载可以作为新任务重新提交
                    t.setStatus(TaskStatus.CANCELLED);
                    Instant now = Instant.now();
                    t.setCancelledAt(now);
                    t.setCancelReason(reason);
                    t.setVersion(t.getVersion() + 1);
                    t.setUpdatedAt(now);
                    idByDedupKey.remove(dedupKey(t.getBusinessKey(), t.getPayload()), t.getId());
                    persistLocked();
                    String text = reason == null || reason.isBlank() ? "任务已取消" : "任务已取消: " + reason;
                    return CancelResult.cancelled(id, text);
                }
                case CANCELLED -> {
                    return CancelResult.alreadyCancelled(id);
                }
                case RUNNING -> {
                    return CancelResult.running(id);
                }
                case SUCCEEDED -> {
                    return CancelResult.alreadyFinished(id, TaskStatus.SUCCEEDED);
                }
                default -> {
                    return CancelResult.alreadyFinished(id, t.getStatus());
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
