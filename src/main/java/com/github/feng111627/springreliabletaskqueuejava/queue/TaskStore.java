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
     * 原子领取到期任务：
     * 资格条件（全部满足）：状态为 PENDING/FAILED；nextAttemptAt &lt;= now；
     * 同一业务键下不存在比它更早创建（createdAt 更小，id 作为次序兜底）的非终结任务
     * （终结 = SUCCEEDED/DEAD/CANCELLED）——保证同键严格先来后到，优先级与定时投递都不能越级。
     * 在有资格的任务中按 priority 降序、createdAt 升序、id 升序领取最多 max 条，
     * 并把它们置为 RUNNING。同一任务只会被一个消费者领到。
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
     * 取消任务：PENDING / FAILED / DEAD -> CANCELLED（终态），释放该任务的幂等键，
     * 使同一业务键+负载可以重新提交。
     * RUNNING 不允许取消；SUCCEEDED/CANCELLED 等终态返回可区分的结果；不存在返回 NOT_FOUND。
     */
    CancelResult cancel(String id, String reason);

    List<TaskRecord> listByStatus(TaskStatus status);

    List<TaskRecord> listAll();
}
