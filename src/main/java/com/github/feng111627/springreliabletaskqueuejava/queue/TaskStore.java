package com.github.feng111627.springreliabletaskqueuejava.queue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 任务存储抽象。本地文件实现模拟队列后端，所有状态变更都是原子的并持久化。
 */
public interface TaskStore {

    /**
     * 幂等插入：幂等键 = 业务键 + 负载。相同幂等键的重复提交返回 DUPLICATE（携带已有任务状态），
     * 相同业务键但负载不同视为不同任务，正常入队（消费时按业务键保序）。
     */
    SubmitResult insertIfAbsent(TaskRecord task);

    Optional<TaskRecord> findByBusinessKey(String businessKey);

    Optional<TaskRecord> findById(String id);

    /**
     * 原子领取到期任务：把 PENDING/FAILED 且 nextAttemptAt <= now 的任务置为 RUNNING。
     * 返回按创建时间排序的最多 max 条，保证同一任务只会被一个消费者领到。
     */
    List<TaskRecord> claimDue(Instant now, int max);

    /** 标记执行成功（仅当仍处于 RUNNING 且版本匹配时生效）。 */
    boolean markSucceeded(String id, long version);

    /**
     * 标记本次执行失败：attempts+1；未耗尽则置 FAILED 并计算下次重试时间，耗尽则置 DEAD。
     */
    boolean markFailed(String id, long version, String error, Instant nextAttemptAt, boolean exhausted);

    /** 启动恢复：把上次进程遗留的 RUNNING 任务回收为 PENDING，返回回收数量。 */
    int recoverRunning();

    /** 死信再投递：DEAD -> PENDING，attempts 清零。 */
    boolean requeueDead(String id);

    /**
     * 取消未终结的任务（PENDING/FAILED/DEAD 可取消；RUNNING/SUCCEEDED/CANCELLED 各有可区分结果）。
     * 取消是原子的：取消后任务不可能再被领取执行。
     */
    CancelResult cancel(String id);

    List<TaskRecord> listByStatus(TaskStatus status);

    List<TaskRecord> listAll();
}
