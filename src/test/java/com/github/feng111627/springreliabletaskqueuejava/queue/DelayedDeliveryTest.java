package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：定时投递（notBefore）。
 * 期望：
 * 1) 到点前任务既不被消费、也不进入失败重试，到点后才执行；
 * 2) 同一业务键下前辈还没到点/没做完时，后辈（即使立即可执行）不能被提前挑走；
 * 3) 重试退避从任务“真正开始执行”之后计算，不与首次延迟混在一起——
 *    延迟很久的任务若首次执行失败，其首次重试仍只等待一个 backoff，而非叠加延迟。
 */
class DelayedDeliveryTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;
    private final List<String> executedAt = new CopyOnWriteArrayList<>();
    private final AtomicInteger execCount = new AtomicInteger(0);
    private volatile boolean failOnce = true;

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        props.setConsumerThreads(1);
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> {
            executedAt.add(task.getBusinessKey() + "|" + task.getPayload());
            if (task.getBusinessKey().equals("backoff") && failOnce && execCount.incrementAndGet() == 1) {
                throw new IllegalStateException("首次执行失败，验证退避从真正执行后算起");
            }
        });
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void taskNotConsumedBeforeNotBeforeAndRunsAfter() throws InterruptedException {
        System.out.println("[CASE] 定时投递：notBefore=约 600ms 后，到点前不得被消费");
        Instant start = Instant.now();
        Instant notBefore = start.plusMillis(600);
        SubmitResult r = service.submit("later", "p", null, notBefore);
        assertEquals(SubmitResult.Outcome.ACCEPTED, r.outcome());
        assertEquals(notBefore, store.findById(r.taskId()).orElseThrow().getNotBefore(),
                "定时点必须持久化在任务记录上");

        dispatcher.start();
        // 到点前等待 300ms，任务必须仍然是 PENDING、一次都没执行
        Thread.sleep(300);
        assertEquals(0, executedAt.size(), "notBefore 未到，任务不能被消费");
        assertEquals(TaskStatus.PENDING, store.findById(r.taskId()).orElseThrow().getStatus(),
                "等待期间任务应保持 PENDING，而不是 FAILED/RUNNING");
        System.out.println("[INFO] 到点前任务保持 PENDING，未被消费");

        TestQueues.await("到点后任务执行成功",
                () -> store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        long elapsed = java.time.Duration.between(start, Instant.now()).toMillis();
        assertTrue(elapsed >= 550, "任务不应在 notBefore 之前执行，实际开始后完成耗时=" + elapsed + "ms");
        assertEquals(1, executedAt.size(), "到点后恰好执行一次");
        System.out.println("[PASS] 任务在 notBefore 到点后才被消费，总耗时约 " + elapsed + "ms");
    }

    @Test
    void delayedHeadOfKeyBlocksYoungerSiblingEvenIfSiblingIsImmediatelyDue() throws InterruptedException {
        System.out.println("[CASE] 同键队头延迟：前辈 notBefore=500ms，后辈立即可执行，后辈必须等前辈做完");
        Instant start = Instant.now();
        SubmitResult older = service.submit("key-D", "older", 0, start.plusMillis(500));
        SubmitResult younger = service.submit("key-D", "younger", 0, null);

        dispatcher.start();
        Thread.sleep(300);
        assertTrue(executedAt.isEmpty(), "前辈未到点时，同键后辈也不能被提前挑走执行");
        System.out.println("[INFO] 前 300ms 同键两个任务都未执行（后辈未提前插队）");

        TestQueues.await("两个任务全部完成",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 2);
        System.out.println("[INFO] 实际消费顺序=" + executedAt);
        assertEquals(List.of("key-D|older", "key-D|younger"), executedAt,
                "前辈到点先做，后辈必须排在其后");
        long elapsed = java.time.Duration.between(start, Instant.now()).toMillis();
        assertTrue(elapsed >= 450, "后辈不得在前辈的 notBefore 之前被执行，实际=" + elapsed + "ms");
        System.out.println("[PASS] 同键前辈延迟期间后辈未被提前挑走，到点后严格按 older -> younger 执行");
    }

    @Test
    void retryBackoffIsCountedFromActualExecutionNotFromSubmitDelay() {
        System.out.println("[CASE] 退避不与延迟混淆：notBefore=400ms 的任务首次执行失败，"
                + "首次重试应只等 backoffBase=" + props.getBackoffBaseMs() + "ms 左右");
        Instant start = Instant.now();
        SubmitResult r = service.submit("backoff", "p", null, start.plusMillis(400));

        dispatcher.start();
        // 首次执行约在 400ms 时发生并失败；第二次执行（首次重试）≈ 400 + backoffBase(50)
        TestQueues.await("重试后执行成功（共执行 2 次）", () -> execCount.get() == 2
                && store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        long elapsed = java.time.Duration.between(start, Instant.now()).toMillis();

        TaskRecord done = store.findById(r.taskId()).orElseThrow();
        assertEquals(1, done.getAttempts(), "首次失败计 1 次，重试成功不再累加 attempts");
        // 若退避错误地从提交时刻/延迟时间算起，第二次执行会再拖 ~400ms；
        // 正确实现下第二次执行 ≈ 400+50，留出轮询抖动余量取上界 900ms。
        assertTrue(elapsed < 900,
                "首次重试应在真正执行失败后只等一个退避(约 50ms)，实际距提交=" + elapsed + "ms");
        System.out.println("[PASS] 延迟 400ms 到点执行 -> 失败 -> 仅退避约 "
                + props.getBackoffBaseMs() + "ms 后重试成功，距提交 " + elapsed + "ms，退避未与延迟叠加");
    }

    @Test
    void pastNotBeforeRunsImmediately() {
        System.out.println("[CASE] notBefore 传过去时间：等价于立即可执行");
        SubmitResult r = service.submit("past", "p", null, Instant.now().minus(1, ChronoUnit.HOURS));
        dispatcher.start();
        TestQueues.await("任务快速执行成功",
                () -> store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(1, executedAt.size());
        System.out.println("[PASS] notBefore 为过去时间时任务立即可执行");
    }
}
