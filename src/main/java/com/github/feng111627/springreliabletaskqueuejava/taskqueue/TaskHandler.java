package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

/** 业务处理器：由使用方提供 Bean，执行任务体；抛异常即视为失败并进入重试 */
@FunctionalInterface
public interface TaskHandler {
    void handle(Task task) throws Exception;
}
