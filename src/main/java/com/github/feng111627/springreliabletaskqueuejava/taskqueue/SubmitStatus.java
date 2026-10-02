package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

/** 提交结果类型：区分真正入队与幂等去重命中 */
public enum SubmitStatus {
    /** 新任务，已入队 */
    ACCEPTED,
    /** 相同业务键的任务已存在且未终结，本次提交被去重 */
    DUPLICATE
}
