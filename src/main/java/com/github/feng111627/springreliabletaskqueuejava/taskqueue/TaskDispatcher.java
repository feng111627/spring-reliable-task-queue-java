package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 消费者调度器。
 * - 启动 consumerCount 个消费者线程并发领取任务；
 * - 通过 inFlightKeys 保证同一业务键同一时间只被一个消费者处理（按业务键保序）；
 * - 执行失败按指数退避重试，重试耗尽由存储层置为 DEAD。
 */
public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    private final TaskStore store;
    private final TaskHandler handler;
    private final TaskQueueProperties properties;
    /** 正在处理中的业务键（保序栅栏），仅由消费者线程在领取前后访问，需同步 */
    private final Set<String> inFlightKeys = new HashSet<>();
    private final java.util.List<Thread> workers = new CopyOnWriteArrayList<>();
    private volatile boolean running = false;
    private CountDownLatch stoppedLatch;

    public TaskDispatcher(TaskStore store, TaskHandler handler, TaskQueueProperties properties) {
        this.store = store;
        this.handler = handler;
        this.properties = properties;
    }

    public void start() {
        running = true;
        stoppedLatch = new CountDownLatch(properties.getConsumerCount());
        for (int i = 0; i < properties.getConsumerCount(); i++) {
            Thread worker = new Thread(this::consumeLoop, "task-consumer-" + i);
            worker.setDaemon(true);
            workers.add(worker);
            worker.start();
        }
        log.info("[调度器] 已启动 {} 个消费者线程", properties.getConsumerCount());
    }

    public void stop() {
        running = false;
        workers.forEach(Thread::interrupt);
        try {
            boolean allStopped = stoppedLatch != null && stoppedLatch.await(10, TimeUnit.SECONDS);
            if (!allStopped) {
                log.warn("[调度器] 停止等待超时，仍有消费者未退出");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("[调度器] 已停止");
    }

    private void consumeLoop() {
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                Optional<Task> claimed;
                synchronized (inFlightKeys) {
                    claimed = store.claimNextDue(inFlightKeys);
                    claimed.ifPresent(t -> inFlightKeys.add(t.getBusinessKey()));
                }
                if (claimed.isEmpty()) {
                    sleepQuietly(properties.getPollIntervalMs());
                    continue;
                }
                Task task = claimed.get();
                try {
                    execute(task);
                } finally {
                    synchronized (inFlightKeys) {
                        inFlightKeys.remove(task.getBusinessKey());
                    }
                }
            }
        } finally {
            if (stoppedLatch != null) {
                stoppedLatch.countDown();
            }
        }
    }

    private void execute(Task task) {
        log.info("[任务执行] id={} businessKey={} 第 {}/{} 次尝试",
                task.getId(), task.getBusinessKey(), task.getAttempts(), task.getMaxAttempts());
        try {
            handler.handle(task);
            store.markSucceeded(task.getId());
            log.info("[任务成功] id={} businessKey={}", task.getId(), task.getBusinessKey());
        } catch (Throwable e) {
            long backoffMs = computeBackoffMs(task.getAttempts());
            Task updated = store.markFailed(task.getId(), e.getClass().getSimpleName() + ": " + e.getMessage(), backoffMs);
            if (updated.getStatus() == TaskStatus.DEAD) {
                log.error("[任务死信] id={} businessKey={} 重试 {} 次耗尽，进入死信。原因: {}",
                        task.getId(), task.getBusinessKey(), task.getAttempts(), updated.getLastError());
            } else {
                log.warn("[任务失败] id={} businessKey={} 第 {} 次尝试失败，{}ms 后重试。原因: {}",
                        task.getId(), task.getBusinessKey(), task.getAttempts(), backoffMs, updated.getLastError());
            }
        }
    }

    /** 指数退避：base * 2^(attempts-1)，封顶 maxBackoffMs */
    private long computeBackoffMs(int attempts) {
        long base = properties.getBackoffBaseMs();
        long shift = Math.max(0, attempts - 1);
        long backoff = shift >= 62 ? Long.MAX_VALUE : base << shift;
        return Math.min(backoff, properties.getMaxBackoffMs());
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
