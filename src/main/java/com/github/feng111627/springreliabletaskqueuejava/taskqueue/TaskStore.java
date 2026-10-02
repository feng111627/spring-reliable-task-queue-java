package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/** 任务存储抽象：本地文件实现，保证重启后状态可恢复 */
public interface TaskStore {

    /** 启动时调用：加载持久化数据并把中断的 RUNNING 任务恢复为 PENDING */
    void recover();

    /** 玩幂等去重：按业务键查找未终结任务 */
    Optional<Task> findActiveByBusinessKey(String businessKey);

    Optional<Task> findById(String id);

    /** 新增任务（调用前需已完成去重判断） */
    Task insert(Task task);

    /**
     * 尝试领取一个到期可执行的任务：PENDING/FAILED 且到达重试时间 -> RUNNING。
     * @param excludedBusinessKeys 正在被其他消费者处理中的业务键，这些键的任务本次跳过，
     *                             保证同一业务键不会被并发领取（按业务键保序）
     */
    Optional<Task> claimNextDue(Set<String> excludedBusinessKeys);

    /** 标记成功 */
    void markSucceeded(String taskId);

    /** 标记失败：未耗尽则置 FAILED 并计算下次重试时间，耗尽则置 DEAD */
    Task markFailed(String taskId, String error, long backoffMs);

    /** 死信再投递：DEAD -> PENDING，attempts 归零 */
    Task redeliver(String taskId);

    List<Task> listDead();

    List<Task> listAll();
}
