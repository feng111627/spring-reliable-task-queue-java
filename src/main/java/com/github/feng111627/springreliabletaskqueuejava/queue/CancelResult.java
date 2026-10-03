package com.github.feng111627.springreliabletaskqueuejava.queue;

/**
 * 取消结果：明确区分“本次取消生效”“重复取消”“任务已终结”“正在执行”“不存在”等情况，
 * 调用方可以据此判断取消是否真正发生。
 */
public record CancelResult(Outcome outcome, String taskId, TaskStatus currentStatus, String reason) {

    public enum Outcome {
        /** 本次调用把任务取消（PENDING / FAILED / DEAD -> CANCELLED） */
        CANCELLED,
        /** 任务此前已经被取消，本次为重复取消 */
        ALREADY_CANCELLED,
        /** 任务已终结（SUCCEEDED），无法取消 */
        ALREADY_FINISHED,
        /** 任务正在执行（RUNNING），不允许取消 */
        RUNNING,
        /** 按 taskId 找不到任务 */
        NOT_FOUND
    }

    public static CancelResult cancelled(String taskId, String reason) {
        return new CancelResult(Outcome.CANCELLED, taskId, TaskStatus.CANCELLED, reason);
    }

    public static CancelResult alreadyCancelled(String taskId) {
        return new CancelResult(Outcome.ALREADY_CANCELLED, taskId, TaskStatus.CANCELLED,
                "任务此前已被取消，本次为重复取消，不会重复生效");
    }

    public static CancelResult alreadyFinished(String taskId, TaskStatus status) {
        return new CancelResult(Outcome.ALREADY_FINISHED, taskId, status,
                "任务已终结（" + status + "），无法取消");
    }

    public static CancelResult running(String taskId) {
        return new CancelResult(Outcome.RUNNING, taskId, TaskStatus.RUNNING,
                "任务正在执行，不允许取消");
    }

    public static CancelResult notFound(String taskId) {
        return new CancelResult(Outcome.NOT_FOUND, taskId, null, "任务不存在: " + taskId);
    }

    public boolean effective() {
        return outcome == Outcome.CANCELLED;
    }
}
