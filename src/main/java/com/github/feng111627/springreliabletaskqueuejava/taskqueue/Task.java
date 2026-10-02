package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

import java.time.Instant;

/** 任务实体，会被持久化到本地存储 */
public class Task {
    private String id;
    private String businessKey;
    private String payload;
    private TaskStatus status;
    private int attempts;
    private int maxAttempts;
    private long nextAttemptAtEpochMs;
    private String lastError;
    private Instant createdAt;
    private Instant updatedAt;

    public Task() {
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getBusinessKey() { return businessKey; }
    public void setBusinessKey(String businessKey) { this.businessKey = businessKey; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getNextAttemptAtEpochMs() { return nextAttemptAtEpochMs; }
    public void setNextAttemptAtEpochMs(long nextAttemptAtEpochMs) { this.nextAttemptAtEpochMs = nextAttemptAtEpochMs; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
