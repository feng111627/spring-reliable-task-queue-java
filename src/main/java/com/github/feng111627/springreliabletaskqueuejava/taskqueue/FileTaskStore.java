package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 基于本地 JSON 文件的任务存储。
 * 内存中维护全量任务表，每次状态变更后原子写盘（临时文件 + rename），
 * 重启时通过 {@link #recover()} 重新加载并把中断的 RUNNING 任务恢复为 PENDING。
 * 所有公共方法加锁，保证多消费者并发下的状态一致性。
 */
public class FileTaskStore implements TaskStore {

    private final Path storageFile;
    private final ObjectMapper objectMapper;
    /** 全量任务表，key 为任务 id，保持插入序 */
    private final Map<String, Task> tasks = new LinkedHashMap<>();

    public FileTaskStore(TaskQueueProperties properties) {
        this.storageFile = Path.of(properties.getStoragePath());
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public synchronized void recover() {
        tasks.clear();
        if (Files.exists(storageFile)) {
            try {
                Task[] loaded = objectMapper.readValue(storageFile.toFile(), Task[].class);
                for (Task task : loaded) {
                    tasks.put(task.getId(), task);
                }
            } catch (IOException e) {
                throw new IllegalStateException("加载任务存储文件失败: " + storageFile, e);
            }
        }
        // 重启恢复：上次进程退出时仍处于 RUNNING 的任务说明执行被中断，重新入队
        int recovered = 0;
        for (Task task : tasks.values()) {
            if (task.getStatus() == TaskStatus.RUNNING) {
                task.setStatus(TaskStatus.PENDING);
                task.setUpdatedAt(Instant.now());
                recovered++;
            }
        }
        if (recovered > 0 || !tasks.isEmpty()) {
            persist();
        }
    }

    @Override
    public synchronized Optional<Task> findActiveByBusinessKey(String businessKey) {
        return tasks.values().stream()
                .filter(t -> t.getBusinessKey().equals(businessKey))
                .filter(t -> t.getStatus() != TaskStatus.SUCCEEDED && t.getStatus() != TaskStatus.DEAD)
                .findFirst()
                .map(this::copy);
    }

    @Override
    public synchronized Optional<Task> findById(String id) {
        return Optional.ofNullable(tasks.get(id)).map(this::copy);
    }

    @Override
    public synchronized Task insert(Task task) {
        tasks.put(task.getId(), task);
        persist();
        return copy(task);
    }

    @Override
    public synchronized Optional<Task> claimNextDue(Set<String> excludedBusinessKeys) {
        long now = System.currentTimeMillis();
        Optional<Task> due = tasks.values().stream()
                .filter(t -> (t.getStatus() == TaskStatus.PENDING || t.getStatus() == TaskStatus.FAILED)
                        && t.getNextAttemptAtEpochMs() <= now)
                .filter(t -> !excludedBusinessKeys.contains(t.getBusinessKey()))
                .min(Comparator.comparing(Task::getCreatedAt));
        due.ifPresent(task -> {
            task.setStatus(TaskStatus.RUNNING);
            task.setAttempts(task.getAttempts() + 1);
            task.setUpdatedAt(Instant.now());
            persist();
        });
        return due.map(this::copy);
    }

    @Override
    public synchronized void markSucceeded(String taskId) {
        Task task = requireTask(taskId);
        task.setStatus(TaskStatus.SUCCEEDED);
        task.setLastError(null);
        task.setUpdatedAt(Instant.now());
        persist();
    }

    @Override
    public synchronized Task markFailed(String taskId, String error, long backoffMs) {
        Task task = requireTask(taskId);
        task.setLastError(error);
        task.setUpdatedAt(Instant.now());
        if (task.getAttempts() >= task.getMaxAttempts()) {
            // 重试耗尽，进入死信
            task.setStatus(TaskStatus.DEAD);
        } else {
            task.setStatus(TaskStatus.FAILED);
            task.setNextAttemptAtEpochMs(System.currentTimeMillis() + backoffMs);
        }
        persist();
        return copy(task);
    }

    @Override
    public synchronized Task redeliver(String taskId) {
        Task task = requireTask(taskId);
        if (task.getStatus() != TaskStatus.DEAD) {
            throw new IllegalStateException("只有死信任务可以再投递，当前状态: " + task.getStatus());
        }
        task.setStatus(TaskStatus.PENDING);
        task.setAttempts(0);
        task.setNextAttemptAtEpochMs(System.currentTimeMillis());
        task.setUpdatedAt(Instant.now());
        persist();
        return copy(task);
    }

    @Override
    public synchronized List<Task> listDead() {
        return tasks.values().stream()
                .filter(t -> t.getStatus() == TaskStatus.DEAD)
                .map(this::copy)
                .toList();
    }

    @Override
    public synchronized List<Task> listAll() {
        return tasks.values().stream().map(this::copy).toList();
    }

    private Task requireTask(String taskId) {
        Task task = tasks.get(taskId);
        if (task == null) {
            throw new IllegalArgumentException("任务不存在: " + taskId);
        }
        return task;
    }

    /** 原子落盘：先写临时文件再 rename，避免写一半崩溃导致文件损坏 */
    private void persist() {
        try {
            Files.createDirectories(storageFile.toAbsolutePath().getParent());
            Path tmp = storageFile.resolveSibling(storageFile.getFileName() + ".tmp");
            objectMapper.writeValue(tmp.toFile(), new ArrayList<>(tasks.values()));
            Files.move(tmp, storageFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化任务存储失败: " + storageFile, e);
        }
    }

    /** 返回副本，避免外部修改内存态 */
    private Task copy(Task src) {
        Task t = new Task();
        t.setId(src.getId());
        t.setBusinessKey(src.getBusinessKey());
        t.setPayload(src.getPayload());
        t.setStatus(src.getStatus());
        t.setAttempts(src.getAttempts());
        t.setMaxAttempts(src.getMaxAttempts());
        t.setNextAttemptAtEpochMs(src.getNextAttemptAtEpochMs());
        t.setLastError(src.getLastError());
        t.setCreatedAt(src.getCreatedAt());
        t.setUpdatedAt(src.getUpdatedAt());
        return t;
    }
}
