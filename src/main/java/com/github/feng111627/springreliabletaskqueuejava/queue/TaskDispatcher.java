package com.github.feng111627.springreliabletaskqueuejava.queue;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 调度器：
 * - 定时轮询领取到期任务（PENDING/FAILED 且到达 nextAttemptAt）；
 * - 按业务键哈希分带到固定单线程执行器：同一业务键永远落到同一条带，严格串行保序；
 *   不同键分布到不同条带，实现并发消费；
 * - 执行失败按指数退避重试，耗尽进入死信；
 * - 启动时回收上次进程遗留的 RUNNING 任务，保证重启不丢任务。
 */
@Component
public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    private final TaskStore store;
    private final QueueProperties properties;

    private volatile TaskHandler handler = task ->
            log.info("[handler] 默认处理器执行任务 businessKey={} payload={}", task.getBusinessKey(), task.getPayload());

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService poller;
    private ExecutorService[] stripes;

    public TaskDispatcher(TaskStore store, QueueProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /** 注册业务处理器（覆盖默认处理器）。 */
    public void setHandler(TaskHandler handler) {
        this.handler = handler;
    }

    @PostConstruct
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        int recovered = store.recoverRunning();
        log.info("[dispatcher] 启动：回收遗留 RUNNING 任务 {} 个，消费者线程数={}",
                recovered, properties.getConsumerThreads());

        int n = properties.getConsumerThreads();
        stripes = new ExecutorService[n];
        for (int i = 0; i < n; i++) {
            final int lane = i;
            stripes[i] = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "queue-consumer-" + lane);
                t.setDaemon(true);
                return t;
            });
        }
        poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "queue-poller");
            t.setDaemon(true);
            return t;
        });
        poller.scheduleWithFixedDelay(this::pollSafely,
                properties.getPollIntervalMs(), properties.getPollIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("[dispatcher] 停止调度器");
        if (poller != null) {
            poller.shutdownNow();
        }
        if (stripes != null) {
            for (ExecutorService stripe : stripes) {
                stripe.shutdown();
            }
            for (ExecutorService stripe : stripes) {
                try {
                    stripe.awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void pollSafely() {
        try {
            poll();
        } catch (Exception e) {
            log.error("[dispatcher] 轮询异常", e);
        }
    }

    private void poll() {
        List<TaskRecord> claimed = store.claimDue(Instant.now(), properties.getPollBatchSize());
        for (TaskRecord task : claimed) {
            int lane = Math.floorMod(task.getBusinessKey().hashCode(), stripes.length);
            stripes[lane].submit(() -> execute(task));
        }
    }

    private void execute(TaskRecord task) {
        try {
            handler.handle(task);
            boolean ok = store.markSucceeded(task.getId(), task.getVersion());
            if (ok) {
                log.info("[execute] 成功 businessKey={} taskId={} attempts={}",
                        task.getBusinessKey(), task.getId(), task.getAttempts() + 1);
            } else {
                log.warn("[execute] 成功但状态已变化(可能重启回收)，跳过置成功 taskId={}", task.getId());
            }
        } catch (Exception e) {
            int nextAttempt = task.getAttempts() + 1;
            boolean exhausted = nextAttempt >= task.getMaxAttempts();
            Instant nextAttemptAt = Instant.now().plus(backoff(nextAttempt));
            boolean ok = store.markFailed(task.getId(), task.getVersion(), e.toString(), nextAttemptAt, exhausted);
            if (ok) {
                if (exhausted) {
                    log.error("[execute] 重试耗尽进入死信 businessKey={} taskId={} attempts={} 错误={}",
                            task.getBusinessKey(), task.getId(), nextAttempt, e.toString());
                } else {
                    log.warn("[execute] 失败待重试 businessKey={} taskId={} 第{}次 下次重试={} 错误={}",
                            task.getBusinessKey(), task.getId(), nextAttempt, nextAttemptAt, e.toString());
                }
            }
        }
    }

    /** 指数退避：base * 2^(attempt-1)，封顶 max。 */
    Duration backoff(int attempt) {
        long delay = properties.getBackoffBaseMs();
        for (int i = 1; i < attempt && delay < properties.getBackoffMaxMs(); i++) {
            delay = Math.min(delay * 2, properties.getBackoffMaxMs());
        }
        return Duration.ofMillis(Math.min(delay, properties.getBackoffMaxMs()));
    }
}
