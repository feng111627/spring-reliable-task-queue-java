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
     * 提交任务。相同业务键+相同负载重复提交时不会重复入队，返回 DUPLICATE 及已有任务的当前状态。
     * 等价于以默认优先级 0、不延迟提交（与历史行为完全一致）。
     */
    public SubmitResult submit(String businessKey, String payload) {
        return submit(businessKey, payload, null, null);
    }

    /**
     * 提交任务（支持优先级与定时投递）。
     *
     * @param priority 优先级，整数越大越优先；null 取默认值 0。
     *                 积压时高优先级先被消费，但同一业务键内部始终严格按提交先后执行。
     * @param notBefore 最早可开始执行的时间点；null 表示入队即可执行。
     *                  到点前任务既不会被消费也不计失败重试；该时间点持久化，重启不丢。
     */
    public SubmitResult submit(String businessKey, String payload, Integer priority, Instant notBefore) {
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
        task.setPriority(priority == null ? 0 : priority);
        task.setNotBefore(notBefore);
        Instant now = Instant.now();
        // 首次可领取时间 = 定时点（若有），否则立即；失败后的退避时间后续只覆盖 nextAttemptAt，不动 notBefore。
        task.setNextAttemptAt(notBefore != null ? notBefore : now);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);
        task.setVersion(0);

        SubmitResult result = store.insertIfAbsent(task);
        if (result.accepted()) {
            log.info("[submit] ACCEPTED businessKey={} taskId={} priority={} notBefore={}",
                    businessKey, task.getId(), task.getPriority(), notBefore);
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
     * 取消任务：排队中(PENDING)、退避重试中(FAILED)、死信(DEAD) 都可取消；
     * 取消后不会再被执行，取消结果可查询，重复取消返回可区分的 ALREADY_CANCELLED。
     */
    public CancelResult cancel(String taskId) {
        CancelResult result = store.cancel(taskId);
        log.info("[cancel] taskId={} 结果={} 原因={}", taskId, result.outcome(), result.reason());
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
