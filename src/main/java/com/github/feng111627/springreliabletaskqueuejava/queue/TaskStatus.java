package com.github.feng111627.springreliabletaskqueuejava.queue;

/**
 * 任务状态机：
 * PENDING   待投递（已入队，等待被消费者领取；若设置了 notBefore，到点前不可领取）
 * RUNNING   已被领取、正在执行（重启时会被回收为 PENDING）
 * FAILED    本次执行失败，等待按退避策略重试（nextAttemptAt 到期后重新投递）
 * SUCCEEDED 执行成功，终态，不再投递
 * DEAD      重试耗尽进入死信，终态，可人工再投递（回到 PENDING）
 * CANCELLED 已被取消，终态，不会被执行；取消后可凭同一业务键重新提交新任务
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    FAILED,
    SUCCEEDED,
    DEAD,
    CANCELLED
}
