package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：多消费者并发消费。
 * 期望：不同业务键并行执行；同一业务键的任务严格按提交顺序串行执行；
 * 每个任务恰好执行一次，不丢不重。
 */
class ConcurrentConsumptionTest {

    private static final int KEYS = 8;
    private static final int TASKS_PER_KEY = 20;

    @TempDir
    Path dir;

    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;

    private final Map<String, List<Integer>> executedSeqByKey = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> inFlightByKey = new ConcurrentHashMap<>();
    private final AtomicInteger maxInFlightForSameKey = new AtomicInteger(0);
    private final AtomicInteger totalExecutions = new AtomicInteger(0);
    private final AtomicInteger currentParallel = new AtomicInteger(0);
    private final AtomicInteger maxParallel = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        QueueProperties props = TestQueues.fastProps(dir.toString());
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> {
            String key = task.getBusinessKey();
            int seq = Integer.parseInt(task.getPayload());
            // 观察全局并发度（不同键应能并行）
            int par = currentParallel.incrementAndGet();
            maxParallel.accumulateAndGet(par, Math::max);
            // 观察同键并发度（必须恒为 1）
            int inFlight = inFlightByKey.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            maxInFlightForSameKey.accumulateAndGet(inFlight, Math::max);
            try {
                Thread.sleep(2); // 放大竞争窗口
                executedSeqByKey.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(seq);
                totalExecutions.incrementAndGet();
            } finally {
                inFlightByKey.get(key).decrementAndGet();
                currentParallel.decrementAndGet();
            }
        });
        dispatcher.start();
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void concurrentConsumptionWithPerKeyOrdering() throws InterruptedException {
        System.out.println("[CASE] 并发消费：" + KEYS + " 个业务键 x " + TASKS_PER_KEY
                + " 个任务，多线程并发提交");
        int total = KEYS * TASKS_PER_KEY;

        // 多线程交错提交，制造并发
        CountDownLatch start = new CountDownLatch(1);
        Thread[] submitters = new Thread[KEYS];
        for (int k = 0; k < KEYS; k++) {
            final String key = "key-" + k;
            submitters[k] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                for (int seq = 0; seq < TASKS_PER_KEY; seq++) {
                    SubmitResult r = service.submit(key, String.valueOf(seq));
                    assertEquals(SubmitResult.Outcome.ACCEPTED, r.outcome());
                }
            });
            submitters[k].start();
        }
        start.countDown();
        for (Thread t : submitters) {
            t.join();
        }

        TestQueues.await("全部任务执行完成",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == total);

        assertEquals(total, totalExecutions.get(), "每个任务必须恰好执行一次");
        assertEquals(1, maxInFlightForSameKey.get(), "同一业务键不得并发执行");
        assertTrue(maxParallel.get() > 1, "不同业务键应能并行执行，实际最大并行度=" + maxParallel.get());

        for (int k = 0; k < KEYS; k++) {
            List<Integer> seqs = executedSeqByKey.get("key-" + k);
            assertNotNull(seqs);
            assertEquals(TASKS_PER_KEY, seqs.size(), "每个键的任务数不能丢");
            for (int i = 0; i < TASKS_PER_KEY; i++) {
                assertEquals(i, seqs.get(i), "同一业务键必须按提交顺序执行: key-" + k);
            }
        }
        System.out.println("[PASS] " + total + " 个任务全部恰好执行一次；同键严格保序（同键最大并发="
                + maxInFlightForSameKey.get() + "）；跨键并行（全局最大并行=" + maxParallel.get() + "）");
    }
}
