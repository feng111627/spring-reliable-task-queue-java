package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 队列配置项。
 */
@ConfigurationProperties(prefix = "queue")
public class QueueProperties {

    /** 本地存储目录 */
    private String storageDir = "./data/queue";

    /** 并发消费者线程数（同时是按业务键保序的分带数） */
    private int consumerThreads = 4;

    /** 最大执行次数（含首次），耗尽后进入死信 */
    private int maxAttempts = 3;

    /** 退避基数（毫秒）：第 n 次重试延迟 = backoffBaseMs * 2^(n-1)，封顶 backoffMaxMs */
    private long backoffBaseMs = 1000;

    /** 退避上限（毫秒） */
    private long backoffMaxMs = 30000;

    /** 调度轮询间隔（毫秒） */
    private long pollIntervalMs = 200;

    /** 单次轮询最多领取的任务数 */
    private int pollBatchSize = 50;

    public String getStorageDir() {
        return storageDir;
    }

    public void setStorageDir(String storageDir) {
        this.storageDir = storageDir;
    }

    public int getConsumerThreads() {
        return consumerThreads;
    }

    public void setConsumerThreads(int consumerThreads) {
        this.consumerThreads = consumerThreads;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffBaseMs() {
        return backoffBaseMs;
    }

    public void setBackoffBaseMs(long backoffBaseMs) {
        this.backoffBaseMs = backoffBaseMs;
    }

    public long getBackoffMaxMs() {
        return backoffMaxMs;
    }

    public void setBackoffMaxMs(long backoffMaxMs) {
        this.backoffMaxMs = backoffMaxMs;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public int getPollBatchSize() {
        return pollBatchSize;
    }

    public void setPollBatchSize(int pollBatchSize) {
        this.pollBatchSize = pollBatchSize;
    }
}
