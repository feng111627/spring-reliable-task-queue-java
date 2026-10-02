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
            List<TaskRecord> due = byId.values().stream()
                    .filter(t -> (t.getStatus() == TaskStatus.PENDING || t.getStatus() == TaskStatus.FAILED)
                            && !t.getNextAttemptAt().isAfter(now))
                    .sorted(Comparator.comparing(TaskRecord::getCreatedAt))
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
