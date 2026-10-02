package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

/** 提交结果：携带结果类型与已存在/新建的任务快照 */
public class SubmitResult {
    private final SubmitStatus status;
    private final Task task;

    public SubmitResult(SubmitStatus status, Task task) {
        this.status = status;
        this.task = task;
    }

    public SubmitStatus getStatus() { return status; }
    public Task getTask() { return task; }

    public boolean isAccepted() { return status == SubmitStatus.ACCEPTED; }
}
