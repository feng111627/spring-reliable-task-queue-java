package com.github.feng111627.springreliabletaskqueuejava.queue;

import java.time.Instant;

/**
 * 任务持久化记录。version 用于乐观锁，避免并发领取/重复投递。
 */
public class TaskRecord {

    private String id;
    private String businessKey;
    private String payload;
    private TaskStatus status;
    private int attempts;
    private int maxAttempts;
    private Instant nextAttemptAt;
    private Instant createdAt;
    private Instant updatedAt;
    private String lastError;
    private long version;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBusinessKey() {
        return businessKey;
    }

    public void setBusinessKey(String businessKey) {
        this.businessKey = businessKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public void setNextAttemptAt(Instant nextAttemptAt) {
        this.nextAttemptAt = nextAttemptAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public TaskRecord copy() {
        TaskRecord c = new TaskRecord();
        c.id = id;
        c.businessKey = businessKey;
        c.payload = payload;
        c.status = status;
        c.attempts = attempts;
        c.maxAttempts = maxAttempts;
        c.nextAttemptAt = nextAttemptAt;
        c.createdAt = createdAt;
        c.updatedAt = updatedAt;
        c.lastError = lastError;
        c.version = version;
        return c;
    }
}
