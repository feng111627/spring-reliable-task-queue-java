package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：任务持续失败。
 * 期望：按配置次数与退避策略重试，耗尽后进入死信且不再投递；死信可查询、可再投递；
 * 重试中的任务不会被重复投递、重复计数。
 */
class RetryAndDeadLetterTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;
    private final ConcurrentHashMap<String, AtomicInteger> executions = new ConcurrentHashMap<>();
    private volatile boolean alwaysFail = true;

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> {
            executions.computeIfAbsent(task.getBusinessKey(), k -> new AtomicInteger()).incrementAndGet();
            if (alwaysFail) {
                throw new IllegalStateException("模拟业务失败: " + task.getBusinessKey());
            }
        });
        dispatcher.start();
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void retriesExhaustedThenDeadLetterThenRedeliver() {
        System.out.println("[CASE] 重试耗尽：处理器持续失败，maxAttempts=" + props.getMaxAttempts());
        service.submit("pay-1", "payload");

        TestQueues.await("任务进入死信",
                () -> store.findByBusinessKey("pay-1").orElseThrow().getStatus() == TaskStatus.DEAD);

        TaskRecord dead = store.findByBusinessKey("pay-1").orElseThrow();
        assertEquals(props.getMaxAttempts(), dead.getAttempts(), "重试次数应恰好等于 maxAttempts");
        assertEquals(props.getMaxAttempts(), executions.get("pay-1").get(), "执行次数不应重复计数");
        assertTrue(dead.getLastError().contains("模拟业务失败"), "死信应保留失败原因");

        List<TaskRecord> deadLetters = service.listDeadLetters();
        assertEquals(1, deadLetters.size(), "死信应可被查出");
        assertEquals("pay-1", deadLetters.get(0).getBusinessKey());
        System.out.println("[PASS] 重试 " + dead.getAttempts() + " 次后进入死信，失败原因=" + dead.getLastError());

        // 死信不再被投递
        int countAfterDead = executions.get("pay-1").get();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(countAfterDead, executions.get("pay-1").get(), "死信任务不得再被投递");

        // 修复处理器后再投递死信
        System.out.println("[CASE] 死信再投递：修复处理器后重新入队");
        alwaysFail = false;
        assertTrue(service.redeliverDeadLetter(dead.getId()));
        TestQueues.await("再投递后执行成功",
                () -> store.findByBusinessKey("pay-1").orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(countAfterDead + 1, executions.get("pay-1").get(), "再投递后应恰好再执行一次");
        System.out.println("[PASS] 死信再投递成功，任务最终 SUCCEEDED");
    }

    @Test
    void retryingTaskIsNotDeliveredConcurrently() {
        System.out.println("[CASE] 重试中的任务不会被并发重复投递");
        service.submit("pay-2", "payload");
        TestQueues.await("任务进入死信",
                () -> store.findByBusinessKey("pay-2").orElseThrow().getStatus() == TaskStatus.DEAD);
        // 若存在重复投递，执行次数会超过 maxAttempts
        assertEquals(props.getMaxAttempts(), executions.get("pay-2").get(),
                "重试过程执行次数必须严格等于 maxAttempts，不得重复投递");
        System.out.println("[PASS] 重试期间无重复投递，执行次数=" + executions.get("pay-2").get());
    }
}
