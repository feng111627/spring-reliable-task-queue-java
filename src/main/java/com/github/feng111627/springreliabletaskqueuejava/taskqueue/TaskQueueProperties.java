package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 任务队列配置项 */
@ConfigurationProperties(prefix = "taskqueue")
public class TaskQueueProperties {
    /** 本地存储文件路径 */
    private String storagePath = "./data/task-queue.json";
    /** 并发消费者数量 */
    private int consumerCount = 4;
    /** 默认最大尝试次数（含首次执行） */
    private int maxAttempts = 3;
    /** 重试退避基数（毫秒），实际等待 = base * 2^(attempts-1)， capped by maxBackoffMs */
    private long backoffBaseMs = 1000;
    /** 退避上限（毫秒） */
    private long maxBackoffMs = 30000;
    /** 消费者轮询间隔（毫秒） */
    private long pollIntervalMs = 100;

    public String getStoragePath() { return storagePath; }
    public void setStoragePath(String storagePath) { this.storagePath = storagePath; }
    public int getConsumerCount() { return consumerCount; }
    public void setConsumerCount(int consumerCount) { this.consumerCount = consumerCount; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getBackoffBaseMs() { return backoffBaseMs; }
    public void setBackoffBaseMs(long backoffBaseMs) { this.backoffBaseMs = backoffBaseMs; }
    public long getMaxBackoffMs() { return maxBackoffMs; }
    public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }
    public long getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
}
