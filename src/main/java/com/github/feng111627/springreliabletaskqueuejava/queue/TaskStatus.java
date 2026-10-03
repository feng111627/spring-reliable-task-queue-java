package com.github.feng111627.springreliabletaskqueuejava.queue;

/**
 * 任务状态机：
 * PENDING   待投递（已入队，等待被消费者领取；定时任务在 notBefore 之前停留在此状态）
 * RUNNING   已被领取、正在执行（重启时会被回收为 PENDING）
 * FAILED    本次执行失败，等待按退避策略重试（nextAttemptAt 到期后重新投递）
 * SUCCEEDED 执行成功，终态，不再投递
 * DEAD      重试耗尽进入死信，终态，可人工再投递（回到 PENDING）或取消（变为 CANCELLED）
 * CANCELLED 已取消，终态，不再投递；取消后同一业务键+负载可以重新提交
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    FAILED,
    SUCCEEDED,
    DEAD,
    CANCELLED
}
