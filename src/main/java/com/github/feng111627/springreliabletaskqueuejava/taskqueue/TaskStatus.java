package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

/**
 * 任务状态机：
 * PENDING -> RUNNING -> SUCCEEDED
 *                    -> FAILED(等待重试) -> PENDING(到点重投)
 *                    -> DEAD(重试耗尽，进入死信)
 */
public enum TaskStatus {
    /** 已入队，等待投递 */
    PENDING,
    /** 已被消费者领取，正在执行 */
    RUNNING,
    /** 执行成功，终态 */
    SUCCEEDED,
    /** 执行失败，等待下次重试 */
    FAILED,
    /** 重试耗尽，进入死信，终态（可人工再投递） */
    DEAD
}
