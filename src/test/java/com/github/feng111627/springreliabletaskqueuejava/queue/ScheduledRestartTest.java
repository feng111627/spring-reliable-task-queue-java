package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：重启后定时点与退避时间都不丢。
 * 1) 提交一个 notBefore 在未来的任务，在到点前“重启进程”：新进程仍须到点后才执行，notBefore 不丢；
 * 2) 一个任务首次执行失败进入 FAILED（nextAttemptAt 在未来），在退避到期前“重启进程”：
 *    重启后不得立刻执行，必须等到原退避时间点，nextAttemptAt 不丢。
 */
class ScheduledRestartTest {

    @TempDir
    Path dir;

    @Test
    void notBeforeSurvivesRestartAndTaskRunsOnlyAfterDue() throws Exception {
        QueueProperties props = TestQueues.fastProps(dir.toString());
        props.setConsumerThreads(1);

        // ---- 第一次进程：提交一个 700ms 后才可执行的任务，提交后立即“崩溃” ----
        FileTaskStore store1 = TestQueues.newStore(props);
        TaskQueueService service1 = new TaskQueueService(store1, props);
        Instant start = Instant.now();
        Instant notBefore = start.plusMillis(700);
        SubmitResult r = service1.submit("delayed-restart", "p", null, notBefore);
        String taskId = r.taskId();

        // ---- 第二次进程：从同一目录恢复，立即开始调度 ----
        FileTaskStore store2 = TestQueues.newStore(props);
        TaskRecord restored = store2.findById(taskId).orElseThrow();
        assertEquals(TaskStatus.PENDING, restored.getStatus());
        assertEquals(notBefore, restored.getNotBefore(), "定时点必须随存储恢复");
        assertFalse(restored.getNextAttemptAt().isBefore(notBefore), "可领取时间也必须恢复到定时点");

        TaskDispatcher dispatcher2 = new TaskDispatcher(store2, props);
        CopyOnWriteArrayList<String> executed = new CopyOnWriteArrayList<>();
        dispatcher2.setHandler(t -> executed.add(t.getBusinessKey()));
        dispatcher2.start();

        Thread.sleep(400);
        assertTrue(executed.isEmpty(), "重启后仍在 notBefore 之前，任务不能被提前执行");
        System.out.println("[CASE] 重启后 400ms 内延迟任务未执行（notBefore 未丢）");

        TestQueues.await("到点后延迟任务被执行", () -> executed.contains("delayed-restart"));
        long elapsed = java.time.Duration.between(start, Instant.now()).toMillis();
        assertTrue(elapsed >= 650, "必须等到 notBefore 附近才执行，实际=" + elapsed + "ms");
        assertEquals(1, executed.size(), "延迟任务重启后恰好执行一次");
        dispatcher2.stop();
        System.out.println("[PASS] notBefore=700ms 的任务在中途重启后仍到点才执行，距提交 " + elapsed + "ms");
    }

    @Test
    void backoffNextAttemptSurvivesRestart() throws Exception {
        QueueProperties props = TestQueues.fastProps(dir.toString());
        props.setConsumerThreads(1);
        props.setBackoffBaseMs(1500); // 首次失败后约 1.5s 才能重试，留出“重启”窗口
        props.setBackoffMaxMs(3000);

        // ---- 第一次进程：任务首次执行失败，进入 FAILED 等待退避，随后“崩溃” ----
        FileTaskStore store1 = TestQueues.newStore(props);
        TaskQueueService service1 = new TaskQueueService(store1, props);
        TaskDispatcher dispatcher1 = new TaskDispatcher(store1, props);
        CopyOnWriteArrayList<String> exec1 = new CopyOnWriteArrayList<>();
        dispatcher1.setHandler(t -> {
            exec1.add(t.getBusinessKey());
            throw new IllegalStateException("首次执行失败");
        });
        dispatcher1.start();
        Instant start = Instant.now();
        service1.submit("retry-restart", "p");
        TestQueues.await("任务进入 FAILED 等待退避",
                () -> store1.findByBusinessKey("retry-restart").orElseThrow().getStatus() == TaskStatus.FAILED);
        Instant nextAttempt = store1.findByBusinessKey("retry-restart").orElseThrow().getNextAttemptAt();
        long expectedWait = java.time.Duration.between(Instant.now(), nextAttempt).toMillis();
        assertTrue(expectedWait > 500, "构造场景：退避尚未到期（剩余约 " + expectedWait + "ms）");
        dispatcher1.stop(); // 模拟进程退出（FAILED 状态及 nextAttemptAt 已落盘）
        System.out.println("[CASE] 任务首次失败进入 FAILED，退避剩余约 " + expectedWait + "ms 时重启进程");

        // ---- 第二次进程：处理器改为成功，期望必须等到原退避点，而不是重启后立即执行 ----
        FileTaskStore store2 = TestQueues.newStore(props);
        TaskRecord restored = store2.findByBusinessKey("retry-restart").orElseThrow();
        assertEquals(TaskStatus.FAILED, restored.getStatus(), "重启后仍应是 FAILED");
        assertEquals(nextAttempt, restored.getNextAttemptAt(), "退避重试时间点必须原样恢复");

        TaskDispatcher dispatcher2 = new TaskDispatcher(store2, props);
        AtomicInteger exec2 = new AtomicInteger(0);
        dispatcher2.setHandler(t -> exec2.incrementAndGet());
        long restartAt = System.nanoTime();
        dispatcher2.start();

        Thread.sleep(400);
        assertEquals(0, exec2.get(), "退避未到期前重启，任务不得被立即执行");
        System.out.println("[INFO] 重启后 400ms 内未执行（退避时间点未丢、未被重置）");

        TestQueues.await("退避到期后重试成功",
                () -> store2.findByBusinessKey("retry-restart").orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        long elapsedMs = (System.nanoTime() - restartAt) / 1_000_000;
        assertEquals(1, exec2.get(), "退避到期后恰好重试一次");
        assertTrue(elapsedMs >= expectedWait - 600,
                "重试必须发生在原退避点附近，重启后实际等待=" + elapsedMs + "ms，原剩余=" + expectedWait + "ms");
        dispatcher2.stop();
        System.out.println("[PASS] 退避时间点重启不丢：重启后等待 " + elapsedMs
                + "ms（原剩余约 " + expectedWait + "ms）才重试成功");
    }
}
