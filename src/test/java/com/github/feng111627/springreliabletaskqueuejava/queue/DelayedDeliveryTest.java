package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：定时投递（notBefore）。
 * 期望：到点之前不被消费、不算失败重试（停留 PENDING、attempts=0）；到点后才执行；
 * 同键前一个任务未到点/未完成时，后续任务不能被提前挑走；
 * 重试退避从任务真正开始执行之后计算，与定时点互不混淆；定时点持久化（跨重启用例见 RestartRecoverySchedulingTest）。
 */
class DelayedDeliveryTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;
    private final List<String> executed = new CopyOnWriteArrayList<>();
    private final AtomicReference<Instant> firstStartAt = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
        AtomicInteger attempts = new AtomicInteger();
        dispatcher.setHandler(task -> {
            firstStartAt.compareAndSet(null, Instant.now());
            executed.add(task.getBusinessKey() + "|" + task.getPayload());
            if ("fail-then-ok".equals(task.getPayload()) && attempts.incrementAndGet() < 2) {
                throw new IllegalStateException("模拟前两次失败");
            }
        });
        dispatcher.start();
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void notExecutedBeforeNotBeforeButExecutedAfter() {
        Instant submitAt = Instant.now();
        Instant notBefore = submitAt.plusMillis(600);
        System.out.println("[CASE] 定时投递：notBefore=" + notBefore + " 之前不得消费，等待期间不算失败");
        SubmitResult r = service.submit("delayed-1", "p", 0, notBefore);
        assertEquals(SubmitResult.Outcome.ACCEPTED, r.outcome());

        // 到点之前：保持 PENDING，attempts=0，且从未执行
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        TaskRecord waiting = store.findById(r.taskId()).orElseThrow();
        assertEquals(TaskStatus.PENDING, waiting.getStatus(), "等待定时点期间必须是 PENDING");
        assertEquals(0, waiting.getAttempts(), "等待定时点不算失败、不占重试次数");
        assertTrue(executed.isEmpty(), "到点之前不得执行");

        TestQueues.await("定时任务到点后执行成功",
                () -> store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        Instant start = firstStartAt.get();
        assertNotNull(start);
        assertFalse(start.isBefore(notBefore.minusMillis(30)),
                "实际开始执行时间不得早于 notBefore（允许少量调度误差）: start=" + start);
        assertEquals(1, executed.size(), "定时任务到点后只执行一次");
        System.out.println("[PASS] 到点前未执行（PENDING/attempts=0），到点后于 " + start + " 开始执行并成功");
    }

    @Test
    void pastNotBeforeRunsImmediately() {
        System.out.println("[CASE] notBefore 传过去时间：等价于立即投递，不影响旧行为");
        SubmitResult r = service.submit("delayed-2", "p", 0, Instant.now().minusSeconds(10));
        TestQueues.await("过去定时点的任务立即执行",
                () -> store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(1, executed.size());
        System.out.println("[PASS] 过去时间点的任务立即被消费");
    }

    @Test
    void sameKeyLaterTaskBlockedUntilDelayedHeadDone() {
        System.out.println("[CASE] 同键保序：前一个任务未到点时，后一个任务不能被提前挑走");
        // 直接用存储层构造，精确控制时间，避免真实等待
        Instant base = Instant.now();
        TaskRecord head = newRecord("order-d", "head", 0, base.plusSeconds(30));
        TaskRecord later = newRecord("order-d", "later", 0, base);
        store.insertIfAbsent(head);
        store.insertIfAbsent(later);

        // later 虽然已经到期，但 head 还没到点：两个都不能领
        assertTrue(store.claimDue(Instant.now(), 10).isEmpty(),
                "同键前序任务未到点时，已到期的后续任务也必须等待");

        // 时间推进到 head 的定时点：只能领到 head
        List<TaskRecord> atHead = store.claimDue(head.getNotBefore(), 10);
        assertEquals(1, atHead.size());
        assertEquals("head", atHead.get(0).getPayload());

        // head 执行中（非终结），later 依旧不能领
        assertTrue(store.claimDue(Instant.now().plusSeconds(60), 10).isEmpty(),
                "同键前序任务执行中，后续任务不能被提前挑走");

        // head 终结后 later 才能领
        store.markSucceeded(atHead.get(0).getId(), atHead.get(0).getVersion());
        List<TaskRecord> rest = store.claimDue(Instant.now().plusSeconds(60), 10);
        assertEquals(1, rest.size());
        assertEquals("later", rest.get(0).getPayload());
        System.out.println("[PASS] later 一直等到 head 到点并成功（终态）后才被领取");
    }

    @Test
    void backoffComputedFromActualExecutionNotFromNotBefore() {
        System.out.println("[CASE] 退避与定时点分离：定时任务失败后的退避从真正执行时刻起算");
        Instant notBefore = Instant.now().plusMillis(300);
        SubmitResult r = service.submit("delayed-3", "fail-then-ok", 0, notBefore);

        // 第一次执行（到点后）失败进入 FAILED：记录退避时间点
        TestQueues.await("首次失败进入退避",
                () -> store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.FAILED);
        TaskRecord failed = store.findById(r.taskId()).orElseThrow();
        Instant firstStart = firstStartAt.get();
        assertNotNull(firstStart);
        assertEquals(1, failed.getAttempts());
        assertEquals(notBefore, failed.getNotBefore(), "原始定时点保留，不被退避覆盖");
        Duration gap = Duration.between(firstStart, failed.getNextAttemptAt());
        assertTrue(gap.toMillis() >= props.getBackoffBaseMs() - 30,
                "退避必须从真正开始执行后算起，实际间隔=" + gap.toMillis() + "ms");
        assertTrue(failed.getNextAttemptAt().isAfter(notBefore),
                "重试时间点必须晚于定时点（二者不能混用）");

        // 第二次成功，任务最终 SUCCEEDED
        TestQueues.await("退避后重试成功",
                () -> store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(2, executed.size(), "首次失败 + 退避后重试成功，共执行 2 次");
        System.out.println("[PASS] 首次执行后 " + gap.toMillis() + "ms 重试（退避基数="
                + props.getBackoffBaseMs() + "ms），定时点保留，最终成功");
    }

    private TaskRecord newRecord(String businessKey, String payload, int priority, Instant notBefore) {
        TaskRecord t = new TaskRecord();
        t.setId(java.util.UUID.randomUUID().toString());
        t.setBusinessKey(businessKey);
        t.setPayload(payload);
        t.setStatus(TaskStatus.PENDING);
        t.setAttempts(0);
        t.setMaxAttempts(props.getMaxAttempts());
        t.setNextAttemptAt(notBefore);
        t.setCreatedAt(Instant.now());
        t.setUpdatedAt(Instant.now());
        t.setVersion(0);
        t.setPriority(priority);
        t.setNotBefore(notBefore);
        return t;
    }
}
