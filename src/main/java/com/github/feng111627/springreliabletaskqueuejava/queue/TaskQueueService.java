package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 对外的任务队列服务：提交（幂等去重）、查询、死信再投递。
 */
@Service
public class TaskQueueService {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueService.class);

    private final TaskStore store;
    private final QueueProperties properties;

    public TaskQueueService(TaskStore store, QueueProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /**
     * 提交任务。相同业务键重复提交时不会重复入队，返回 DUPLICATE 及已有任务的当前状态。
     */
    public SubmitResult submit(String businessKey, String payload) {
        if (businessKey == null || businessKey.isBlank()) {
            throw new IllegalArgumentException("businessKey 不能为空");
        }
        TaskRecord task = new TaskRecord();
        task.setId(UUID.randomUUID().toString());
        task.setBusinessKey(businessKey);
        task.setPayload(payload);
        task.setStatus(TaskStatus.PENDING);
        task.setAttempts(0);
        task.setMaxAttempts(properties.getMaxAttempts());
        Instant now = Instant.now();
        task.setNextAttemptAt(now);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);
        task.setVersion(0);

        SubmitResult result = store.insertIfAbsent(task);
        if (result.accepted()) {
            log.info("[submit] ACCEPTED businessKey={} taskId={}", businessKey, task.getId());
        } else {
            log.info("[submit] DUPLICATE businessKey={} 已有任务 taskId={} 状态={}",
                    businessKey, result.taskId(), result.currentStatus());
        }
        return result;
    }

    public Optional<TaskRecord> findByBusinessKey(String businessKey) {
        return store.findByBusinessKey(businessKey);
    }

    public List<TaskRecord> listDeadLetters() {
        return store.listByStatus(TaskStatus.DEAD);
    }

    /**
     * 死信再投递：回到 PENDING、重试计数清零，等待调度器重新消费。
     */
    public boolean redeliverDeadLetter(String taskId) {
        boolean ok = store.requeueDead(taskId);
        log.info("[dead-letter] 再投递 taskId={} 结果={}", taskId, ok ? "已重新入队" : "失败(不存在或不是死信)");
        return ok;
    }

    public List<TaskRecord> listAll() {
        return store.listAll();
    }
}
