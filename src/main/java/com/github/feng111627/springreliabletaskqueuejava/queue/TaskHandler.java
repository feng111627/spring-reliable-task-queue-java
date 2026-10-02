package com.github.feng111627.springreliabletaskqueuejava.queue;

/**
 * 业务处理器：抛异常即视为本次执行失败，将进入重试/死信流程。
 */
@FunctionalInterface
public interface TaskHandler {

    void handle(TaskRecord task) throws Exception;
}
