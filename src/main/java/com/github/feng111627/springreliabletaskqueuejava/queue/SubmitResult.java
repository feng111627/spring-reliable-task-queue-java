package com.github.feng111627.springreliabletaskqueuejava.queue;

/**
 * 提交结果：明确区分“真正入队”与“幂等去重命中”，调用方可以据此判断任务是否会被执行。
 */
public record SubmitResult(Outcome outcome, String taskId, TaskStatus currentStatus, String reason) {

    public enum Outcome {
        /** 首次提交，任务已入队，将被执行一次 */
        ACCEPTED,
        /** 相同业务键已存在，本次提交被去重，不会再次执行 */
        DUPLICATE
    }

    public static SubmitResult accepted(String taskId) {
        return new SubmitResult(Outcome.ACCEPTED, taskId, TaskStatus.PENDING, "任务已入队");
    }

    public static SubmitResult duplicate(String taskId, TaskStatus currentStatus) {
        return new SubmitResult(Outcome.DUPLICATE, taskId, currentStatus,
                "相同业务键+相同负载的任务已存在，状态=" + currentStatus + "，本次提交被去重");
    }

    public boolean accepted() {
        return outcome == Outcome.ACCEPTED;
    }
}
