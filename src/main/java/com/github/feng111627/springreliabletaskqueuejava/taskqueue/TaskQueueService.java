package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** 任务队列对外服务：提交（幂等去重）、查询、死信管理 */
public class TaskQueueService {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueService.class);

    private final TaskStore store;
    private final TaskQueueProperties properties;

    public TaskQueueService(TaskStore store, TaskQueueProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /**
     * 提交任务。幂等规则：同一业务键只要存在未终结任务（PENDING/RUNNING/FAILED），
     * 本次提交即被识别为 DUPLICATE 并返回已存在的任务，不会再次入队执行。
     */
    public synchronized SubmitResult submit(String businessKey, String payload) {
        if (businessKey == null || businessKey.isBlank()) {
            throw new IllegalArgumentException("businessKey 不能为空");
        }
        var existing = store.findActiveByBusinessKey(businessKey);
        if (existing.isPresent()) {
            log.info("[提交去重] businessKey={} 已存在未终结任务 id={}, 状态={}, 本次提交被忽略",
                    businessKey, existing.get().getId(), existing.get().getStatus());
            return new SubmitResult(SubmitStatus.DUPLICATE, existing.get());
        }
        Task task = new Task();
        task.setId(UUID.randomUUID().toString());
        task.setBusinessKey(businessKey);
        task.setPayload(payload);
        task.setStatus(TaskStatus.PENDING);
        task.setAttempts(0);
        task.setMaxAttempts(properties.getMaxAttempts());
        task.setNextAttemptAtEpochMs(System.currentTimeMillis());
        task.setCreatedAt(Instant.now());
        task.setUpdatedAt(task.getCreatedAt());
        Task saved = store.insert(task);
        log.info("[提交入队] businessKey={} 新任务 id={}", businessKey, saved.getId());
        return new SubmitResult(SubmitStatus.ACCEPTED, saved);
    }

    public Task getTask(String id) {
        return store.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("任务不存在: " + id));
    }

    public List<Task> listDeadLetters() {
        return store.listDead();
    }

    /** 死信再投递：重置为 PENDING，attempts 归零，可再次按 maxAttempts 重试 */
    public Task redeliver(String id) {
        Task task = store.redeliver(id);
        log.info("[死信再投递] 任务 id={} businessKey={} 已重新入队", task.getId(), task.getBusinessKey());
        return task;
    }

    public List<Task> listAll() {
        return store.listAll();
    }
}
