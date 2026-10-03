package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景（调度/运维扩展）：重启恢复对“定时投递点”和“退避等待”的持久化。
 * 期望：重启后未到 notBefore 的任务依旧不提前执行、到点照常执行（定时点不丢）；
 * 失败任务的退避时间点与 attempts 不丢，重启后严格按原节奏重试，不提前、不重置。
 */
class RestartRecoverySchedulingTest {

    @TempDir
    Path dir;

    @Test
    void notBeforeSurvivesRestartAndTaskRunsOnTimeAfter() {
        QueueProperties props = TestQueues.fastProps(dir.toString());
        Instant notBefore = Instant.now().plusSeconds(3600);

        // ---- 第一次进程：提交一个远未到点的定时任务 ----
        FileTaskStore store1 = TestQueues.newStore(props);
        TaskQueueService service1 = new TaskQueueService(store1, props);
        SubmitResult r = service1.submit("scheduled-1", "p", 5, notBefore);
        TaskRecord before = store1.findById(r.taskId()).orElseThrow();
        assertEquals(TaskStatus.PENDING, before.getStatus());
        assertEquals(notBefore, before.getNextAttemptAt());

        // ---- 重启：新进程从同一存储加载（先不起 dispatcher，直接核对持久化状态）----
        FileTaskStore store2 = TestQueues.newStore(props);
        TaskRecord restored = store2.findById(r.taskId()).orElseThrow();
        assertEquals(TaskStatus.PENDING, restored.getStatus(), "重启后定时任务仍应为 PENDING");
        assertEquals(notBefore, restored.getNotBefore(), "定时投递点必须持久化、不丢失");
        assertEquals(notBefore, restored.getNextAttemptAt(), "到期判断时间点必须保留");
        assertEquals(5, restored.getPriority(), "优先级也要持久化");
        assertEquals(0, restored.getAttempts(), "等待定时点不算失败");

        TaskDispatcher dispatcher2 = new TaskDispatcher(store2, props);
        List<String> executed = new CopyOnWriteArrayList<>();
        dispatcher2.setHandler(task -> executed.add(task.getBusinessKey()));
        dispatcher2.start();
        try {
            Thread.sleep(400); // 远超轮询周期
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertTrue(executed.isEmpty(), "重启后未到点的定时任务仍不得提前执行");
        dispatcher2.stop();

        // ---- 再次重启：这次把定时点模拟为“即将到点”（通过存储层重建到点场景，验证到点即执行）----
        FileTaskStore store3 = TestQueues.newStore(props);
        TaskRecord t3 = store3.findById(r.taskId()).orElseThrow();
        Instant due = Instant.now().plusMillis(300);
        t3.setNextAttemptAt(due);
        t3.setNotBefore(due);
        store3.updateForTest(t3);

        TaskDispatcher dispatcher3 = new TaskDispatcher(store3, props);
        dispatcher3.setHandler(task -> executed.add(task.getBusinessKey()));
        Instant started = Instant.now();
        dispatcher3.start();
        TestQueues.await("重启后定时点到达，任务被执行", () -> executed.contains("scheduled-1"));
        Instant ranAt = Instant.now();
        dispatcher3.stop();

        assertFalse(ranAt.isBefore(due.minusMillis(30)), "任务不得早于持久化的定时点执行: ranAt=" + ranAt);
        assertTrue(Duration.between(started, ranAt).toMillis() >= 250,
                "任务必须等到定时点附近才执行，而不是重启就立即执行");
        assertEquals(1, executed.size(), "定时任务到点后只执行一次");
        System.out.println("[PASS] 定时点跨重启持久化（notBefore/priority/attempts 保留），"
                + "未到点不执行、到点后恰好执行一次");
    }

    @Test
    void backoffScheduleSurvivesRestart() throws Exception {
        QueueProperties props = TestQueues.fastProps(dir.toString());

        // ---- 第一次进程：让任务失败 3 次进入死信前的最后一次 FAILED 等待（maxAttempts=3）----
        FileTaskStore store1 = TestQueues.newStore(props);
        TaskQueueService service1 = new TaskQueueService(store1, props);
        TaskDispatcher dispatcher1 = new TaskDispatcher(store1, props);
        AtomicInteger calls1 = new AtomicInteger();
        dispatcher1.setHandler(task -> {
            calls1.incrementAndGet();
            throw new IllegalStateException("始终失败: " + task.getBusinessKey());
        });
        dispatcher1.start();
        SubmitResult r = service1.submit("backoff-restart", "p");
        // 等到至少失败 1 次、处于 FAILED 退避中（抢占在第二次重试之前）
        TestQueues.await("任务失败进入 FAILED 退避",
                () -> store1.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.FAILED);
        // 停在第一次失败后的退避窗口：立刻停调度器，阻止后续重试（停止后不再轮询、stripe 排空）
        dispatcher1.stop();
        TaskRecord failed = store1.findById(r.taskId()).orElseThrow();
        int attemptsPersisted = failed.getAttempts();
        Instant nextRetry = failed.getNextAttemptAt();
        assertTrue(attemptsPersisted >= 1, "至少应已失败一次，实际=" + attemptsPersisted);
        assertEquals(TaskStatus.FAILED, failed.getStatus());
        assertTrue(nextRetry.isAfter(Instant.now()), "持久化的退避重试点必须在未来，实际=" + nextRetry);

        // ---- 重启：退避点与 attempts 必须原样保留，且在该时间点之前不提前重试 ----
        FileTaskStore store2 = TestQueues.newStore(props);
        TaskRecord restored = store2.findById(r.taskId()).orElseThrow();
        assertEquals(TaskStatus.FAILED, restored.getStatus(), "重启后任务仍应处于 FAILED");
        assertEquals(attemptsPersisted, restored.getAttempts(), "退避中的 attempts 必须保留、不重置");
        assertEquals(nextRetry, restored.getNextAttemptAt(), "退避重试时间点必须原样持久化");

        AtomicInteger calls2 = new AtomicInteger();
        TaskDispatcher dispatcher2 = new TaskDispatcher(store2, props);
        dispatcher2.setHandler(task -> {
            calls2.incrementAndGet();
            throw new IllegalStateException("重启后仍失败: " + task.getBusinessKey());
        });
        Instant restartAt = Instant.now();
        dispatcher2.start();

        // 在持久化的重试点之前，不得发生新的执行
        long waitMs = Math.max(0, Duration.between(Instant.now(), nextRetry).toMillis() - 20);
        if (waitMs > 0) {
            Thread.sleep(waitMs);
        }
        assertEquals(0, calls2.get(), "退避时间点未到，重启后不得提前重试");

        // 退避点之后：恢复重试，attempts 在已有次数上继续累加（不从头计），最终进死信
        TestQueues.await("重启后按原节奏重试并耗尽进入死信",
                () -> store2.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.DEAD);
        TaskRecord dead = store2.findById(r.taskId()).orElseThrow();
        assertEquals(props.getMaxAttempts(), dead.getAttempts(),
                "总尝试次数必须恰好等于 maxAttempts（退避与 attempts 跨重启连续）");
        dispatcher2.stop();
        long elapsed = Duration.between(restartAt, Instant.now()).toMillis();
        assertTrue(calls2.get() >= 1, "重启后必须真正发生过重试");
        System.out.println("[PASS] 退避点/attempts 跨重启保留：重启后 " + elapsed
                + "ms 内未提前重试，到点继续重试，总尝试=" + dead.getAttempts() + " 后进死信");
    }
}
