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
    /** 最早可开始执行的时间点（定时投递）；为 null 表示入队即可执行。重试退避不修改它。 */
    private Instant notBefore;
    /** 到期可领取的时间点：首次取 notBefore/入队时间，失败后退避算出的下次重试时间覆盖它。 */
    private Instant nextAttemptAt;
    /** 优先级，整数越大越优先；默认 0。 */
    private int priority;
    /** 同一业务键内单调递增的提交序号，用于“同键严格先来后到”的领取判定。 */
    private long seq;
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

    public Instant getNotBefore() {
        return notBefore;
    }

    public void setNotBefore(Instant notBefore) {
        this.notBefore = notBefore;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
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
        c.notBefore = notBefore;
        c.nextAttemptAt = nextAttemptAt;
        c.priority = priority;
        c.seq = seq;
        c.createdAt = createdAt;
        c.updatedAt = updatedAt;
        c.lastError = lastError;
        c.version = version;
        return c;
    }
}
