package com.github.feng111627.springreliabletaskqueuejava.queue;

/**
 * 取消结果：明确区分“本次真正取消”“任务早已取消（重复取消）”“任务已终结/执行中无法取消”“任务不存在”，
 * 调用方可以据此给出可区分的处理。
 */
public record CancelResult(Outcome outcome, String taskId, TaskStatus currentStatus, String reason) {

    public enum Outcome {
        /** 本次调用真正把未终结任务置为 CANCELLED */
        CANCELLED,
        /** 任务此前已经是 CANCELLED（重复取消，幂等） */
        ALREADY_CANCELLED,
        /** 任务已执行成功，终态不可取消 */
        ALREADY_SUCCEEDED,
        /** 任务在死信/重试/排队之外的不可取消情形（此处主要保留给正在执行的任务） */
        NOT_CANCELLABLE,
        /** 指定任务不存在 */
        NOT_FOUND
    }

    public static CancelResult cancelled(String taskId) {
        return new CancelResult(Outcome.CANCELLED, taskId, TaskStatus.CANCELLED, "任务已取消，不会再被执行");
    }

    public static CancelResult alreadyCancelled(String taskId) {
        return new CancelResult(Outcome.ALREADY_CANCELLED, taskId, TaskStatus.CANCELLED,
                "任务此前已被取消，本次为重复取消");
    }

    public static CancelResult alreadySucceeded(String taskId) {
        return new CancelResult(Outcome.ALREADY_SUCCEEDED, taskId, TaskStatus.SUCCEEDED,
                "任务已执行成功，终态不可取消");
    }

    public static CancelResult running(String taskId) {
        return new CancelResult(Outcome.NOT_CANCELLABLE, taskId, TaskStatus.RUNNING,
                "任务正在执行中，无法取消（进程内队列不支持执行中断）");
    }

    public static CancelResult notFound(String taskId) {
        return new CancelResult(Outcome.NOT_FOUND, taskId, null, "任务不存在: " + taskId);
    }

    public boolean effective() {
        return outcome == Outcome.CANCELLED;
    }
}
