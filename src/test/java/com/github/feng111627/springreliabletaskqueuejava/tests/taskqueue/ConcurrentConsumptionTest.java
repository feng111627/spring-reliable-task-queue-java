package com.github.feng111627.springreliabletaskqueuejava.tests.taskqueue;

import com.github.feng111627.springreliabletaskqueuejava.taskqueue.FileTaskStore;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.Task;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskDispatcher;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskQueueProperties;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskQueueService;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用例：并发消费与按业务键保序。
 * 多消费者并发时，每个任务恰好执行一次；同一业务键的多个任务保持先后、不并发。
 */
class ConcurrentConsumptionTest {

    @TempDir
    Path tempDir;

    private TaskQueueProperties properties;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        properties = new TaskQueueProperties();
        properties.setStoragePath(tempDir.resolve("queue.json").toString());
        properties.setConsumerCount(4);
        properties.setPollIntervalMs(10);
        properties.setBackoffBaseMs(50);
        properties.setMaxBackoffMs(200);
        store = new FileTaskStore(properties);
        store.recover();
        service = new TaskQueueService(store, properties);
    }

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.stop();
        }
    }

    @Test
    void manyTasksConsumedExactlyOnceByMultipleConsumers() {
        System.out.println("[用例] 并发消费：40 个任务、4 个消费者，每个任务恰好执行一次");
        Map<String, AtomicInteger> executions = new ConcurrentHashMap<>();
        dispatcher = new TaskDispatcher(store, task -> {
            executions.computeIfAbsent(task.getId(), k -> new AtomicInteger()).incrementAndGet();
            Thread.sleep(10);
        }, properties);
        dispatcher.start();

        int total = 40;
        List<String> ids = new CopyOnWriteArrayList<>();
        for (int i = 0; i < total; i++) {
            ids.add(service.submit("key-" + i, "payload-" + i).getTask().getId());
        }

        TestSupport.await("全部任务成功", Duration.ofSeconds(30), () ->
                service.listAll().stream().allMatch(t -> t.getStatus() == TaskStatus.SUCCEEDED));

        long executedOnce = executions.values().stream().filter(c -> c.get() == 1).count();
        System.out.println("[结果] 任务数=" + total + ", 恰好执行一次的任务数=" + executedOnce);
        assertEquals(total, executions.size());
        assertEquals(total, executedOnce, "存在被重复执行或漏执行的任务");
        System.out.println("[通过] 并发消费下每个任务恰好执行一次");
    }

    @Test
    void sameBusinessKeyTasksRunInOrderAndNeverConcurrently() {
        System.out.println("[用例] 按业务键保序：同键两个任务（死信再投递 + 新提交）必须按提交先后执行");
        // 任务 A 先进死信；此后同键可提交任务 B；再投递 A 后，A、B 同为 PENDING，
        // 多消费者下必须 A 先 B 后，且不得并发执行。
        AtomicInteger attempts = new AtomicInteger();
        List<String> executionLog = new CopyOnWriteArrayList<>();
        Map<String, Long> startAt = new ConcurrentHashMap<>();
        Map<String, Long> endAt = new ConcurrentHashMap<>();
        dispatcher = new TaskDispatcher(store, task -> {
            String tag = task.getPayload();
            // 只有 A 的首次投递失败进死信，之后都成功
            if ("A".equals(tag) && attempts.incrementAndGet() <= 3) {
                throw new RuntimeException("A 首次投递失败");
            }
            startAt.put(tag, System.nanoTime());
            Thread.sleep(100); // 拉长执行窗口，放大并发冲突概率
            executionLog.add(tag);
            endAt.put(tag, System.nanoTime());
        }, properties);
        dispatcher.start();

        String idA = service.submit("shared-key", "A").getTask().getId();
        TestSupport.await("任务 A 进入死信", () -> service.getTask(idA).getStatus() == TaskStatus.DEAD);

        // 先停掉调度器，保证 B 提交与 A 再投递都完成后才恢复消费，消除时序偶发
        dispatcher.stop();
        String idB = service.submit("shared-key", "B").getTask().getId();
        service.redeliver(idA);
        System.out.println("[场景] A 已再投递，A(" + idA + ") 与 B(" + idB + ") 同为 PENDING，重启消费者");
        dispatcher.start();

        TestSupport.await("A、B 均执行成功", Duration.ofSeconds(15), () ->
                service.getTask(idA).getStatus() == TaskStatus.SUCCEEDED
                        && service.getTask(idB).getStatus() == TaskStatus.SUCCEEDED);

        System.out.println("[结果] 执行顺序=" + executionLog);
        assertEquals(List.of("A", "B"), executionLog, "同键任务必须按创建先后执行");
        assertTrue(endAt.get("A") <= startAt.get("B"), "同键任务不得并发执行");
        System.out.println("[通过] 同键任务保序且无并发重叠");
    }

    @Test
    void storeSkipsInFlightBusinessKeys() {
        System.out.println("[用例] 存储层保序栅栏：claimNextDue 跳过在处理中的业务键");
        Task t1 = pendingTask("k1", "p1");
        Task t2 = pendingTask("k1", "p2");
        Task t3 = pendingTask("k2", "p3");
        store.insert(t1);
        store.insert(t2);
        store.insert(t3);

        var claimed = store.claimNextDue(java.util.Set.of("k1"));
        System.out.println("[结果] 排除 k1 后领取到 businessKey=" + claimed.map(Task::getBusinessKey));
        assertTrue(claimed.isPresent());
        assertEquals("k2", claimed.get().getBusinessKey(), "k1 在处理中时其任务不得被领取");
        System.out.println("[通过] 在处理中的业务键被正确跳过");
    }

    private Task pendingTask(String key, String payload) {
        Task t = new Task();
        t.setId(java.util.UUID.randomUUID().toString());
        t.setBusinessKey(key);
        t.setPayload(payload);
        t.setStatus(TaskStatus.PENDING);
        t.setMaxAttempts(3);
        t.setNextAttemptAtEpochMs(System.currentTimeMillis());
        t.setCreatedAt(Instant.now());
        t.setUpdatedAt(Instant.now());
        return t;
    }
}
