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
     * 提交任务（默认优先级 0、不定时），行为与旧版本完全一致。
     */
    public SubmitResult submit(String businessKey, String payload) {
        return submit(businessKey, payload, 0, null);
    }

    /**
     * 提交任务。
     *
     * @param priority  优先级，整数越大越优先；不传/传 0 时与旧行为一致（FIFO）
     * @param notBefore 定时投递点：到点之前任务不可被消费、也不算失败重试；null 表示立即投递。
     *                  退避只从任务真正开始执行（领取）之后计算，与该定时点互不影响。
     */
    public SubmitResult submit(String businessKey, String payload, int priority, Instant notBefore) {
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
        // 定时任务：到点之前 nextAttemptAt = notBefore，不会被领取，也不算失败重试
        Instant firstAvailable = (notBefore != null && notBefore.isAfter(now)) ? notBefore : now;
        task.setNextAttemptAt(firstAvailable);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);
        task.setVersion(0);
        task.setPriority(priority);
        task.setNotBefore(notBefore);

        SubmitResult result = store.insertIfAbsent(task);
        if (result.accepted()) {
            log.info("[submit] ACCEPTED businessKey={} taskId={} priority={} notBefore={}",
                    businessKey, task.getId(), priority, notBefore);
        } else {
            log.info("[submit] DUPLICATE businessKey={} 已有任务 taskId={} 状态={}",
                    businessKey, result.taskId(), result.currentStatus());
        }
        return result;
    }

    public Optional<TaskRecord> findByBusinessKey(String businessKey) {
        return store.findByBusinessKey(businessKey);
    }

    public Optional<TaskRecord> findById(String taskId) {
        return store.findById(taskId);
    }

    /**
     * 取消任务（PENDING/FAILED/DEAD 可取消）。结果可区分：本次生效、重复取消、执行中、已终结、不存在。
     */
    public CancelResult cancel(String taskId) {
        return cancel(taskId, null);
    }

    public CancelResult cancel(String taskId, String reason) {
        CancelResult result = store.cancel(taskId, reason);
        switch (result.outcome()) {
            case CANCELLED -> log.info("[cancel] 已取消 taskId={} 状态={}", taskId, result.currentStatus());
            case ALREADY_CANCELLED -> log.info("[cancel] 重复取消 taskId={}（此前已是 CANCELLED）", taskId);
            case RUNNING -> log.info("[cancel] 取消被拒绝 taskId={}（正在执行）", taskId);
            case ALREADY_FINISHED -> log.info("[cancel] 取消被拒绝 taskId={}（已终结 {}）", taskId, result.currentStatus());
            case NOT_FOUND -> log.info("[cancel] 任务不存在 taskId={}", taskId);
        }
        return result;
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
