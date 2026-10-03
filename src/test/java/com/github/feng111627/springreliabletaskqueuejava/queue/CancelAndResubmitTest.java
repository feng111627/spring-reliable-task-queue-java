package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：取消与重新提交。
 * 期望：排队中/重试中/死信任务均可取消且之后不再执行；取消结果可查询；
 * 重复取消、执行中、已成功、不存在分别给出可区分的结果；
 * 取消后同一业务键（同负载）可重新提交为新任务正常执行，同键先后次序确定可预期。
 */
class CancelAndResubmitTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;
    private final List<String> executed = new CopyOnWriteArrayList<>();
    private final AtomicInteger failureMode = new AtomicInteger(0); // 0=成功 1=恒失败

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> {
            executed.add(task.getBusinessKey() + "|" + task.getPayload());
            if (failureMode.get() == 1) {
                throw new IllegalStateException("模拟失败: " + task.getBusinessKey());
            }
        });
        dispatcher.start();
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void cancelPendingTaskPreventsExecution() {
        System.out.println("[CASE] 取消排队中任务：取消后永远不执行");
        // 用 60s 后的定时任务，确定性地保证取消时任务仍处于 PENDING（未被调度器领取）
        SubmitResult r = service.submit("cancel-pending", "p", 0, Instant.now().plusSeconds(60));
        CancelResult c = service.cancel(r.taskId(), "用户撤单");
        assertEquals(CancelResult.Outcome.CANCELLED, c.outcome());

        TaskRecord cancelled = store.findById(r.taskId()).orElseThrow();
        assertEquals(TaskStatus.CANCELLED, cancelled.getStatus());
        assertNotNull(cancelled.getCancelledAt());
        assertTrue(cancelled.getCancelReason().contains("用户撤单"));

        try {
            Thread.sleep(400);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertTrue(executed.isEmpty(), "已取消的排队任务不得执行");
        assertEquals(TaskStatus.CANCELLED, store.findById(r.taskId()).orElseThrow().getStatus());
        System.out.println("[PASS] 排队任务取消后停留 CANCELLED 终态，未被执行，取消时间/原因已落盘");
    }

    @Test
    void cancelDeadLetterThenResubmit() {
        System.out.println("[CASE] 取消死信中的任务");
        failureMode.set(1);
        SubmitResult r = service.submit("cancel-dead", "p");
        TestQueues.await("任务进入死信",
                () -> store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.DEAD);

        CancelResult c = service.cancel(r.taskId(), null);
        assertEquals(CancelResult.Outcome.CANCELLED, c.outcome());
        assertEquals(TaskStatus.CANCELLED, store.findById(r.taskId()).orElseThrow().getStatus());
        assertTrue(service.listDeadLetters().stream().noneMatch(t -> t.getId().equals(r.taskId())),
                "取消后不应再出现在死信列表");
        System.out.println("[PASS] 死信任务可取消并从死信列表移除");
    }

    @Test
    void cancelRetryingTaskStopsFurtherDelivery() {
        System.out.println("[CASE] 取消重试中的任务：停止后续重试");
        failureMode.set(1);
        SubmitResult r = service.submit("cancel-retry", "p");
        TestQueues.await("任务首次失败进入重试",
                () -> store.findById(r.taskId()).orElseThrow().getStatus() == TaskStatus.FAILED);
        int attemptsSeen;
        TaskRecord failed = store.findById(r.taskId()).orElseThrow();
        attemptsSeen = failed.getAttempts();
        assertTrue(attemptsSeen >= 1);

        CancelResult c = service.cancel(r.taskId(), null);
        assertEquals(CancelResult.Outcome.CANCELLED, c.outcome());

        try {
            Thread.sleep(500); // 远超退避上限（200ms），若未取消应继续重试
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        TaskRecord after = store.findById(r.taskId()).orElseThrow();
        assertEquals(TaskStatus.CANCELLED, after.getStatus());
        assertEquals(attemptsSeen, after.getAttempts(), "取消后不得再发生重试");
        System.out.println("[PASS] FAILED 任务取消后停止重试，attempts 停在 " + attemptsSeen);
    }

    @Test
    void repeatedCancelAndOtherStatesAreDistinguishable() {
        System.out.println("[CASE] 取消结果可区分：重复取消/执行中/已成功/不存在");
        SubmitResult r = service.submit("cancel-states", "p");
        String id = r.taskId();

        // 首次取消生效；再次取消 = ALREADY_CANCELLED
        assertEquals(CancelResult.Outcome.CANCELLED, service.cancel(id, null).outcome());
        CancelResult again = service.cancel(id, null);
        assertEquals(CancelResult.Outcome.ALREADY_CANCELLED, again.outcome());
        assertEquals(TaskStatus.CANCELLED, again.currentStatus());
        assertTrue(again.reason().contains("重复取消"));

        // 不存在
        assertEquals(CancelResult.Outcome.NOT_FOUND, service.cancel("no-such-id", null).outcome());

        // 已成功任务不能取消
        failureMode.set(0);
        SubmitResult ok = service.submit("succeeded-1", "p");
        TestQueues.await("succeeded-1 成功",
                () -> store.findById(ok.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(CancelResult.Outcome.ALREADY_FINISHED, service.cancel(ok.taskId(), null).outcome());

        // 执行中（RUNNING）不能取消：直接用存储层把任务置成 RUNNING 来确定地覆盖该分支
        SubmitResult running = service.submit("running-1", "p");
        List<TaskRecord> claimed = store.claimDue(Instant.now(), 10).stream()
                .filter(t -> t.getId().equals(running.taskId())).toList();
        assertEquals(1, claimed.size());
        assertEquals(CancelResult.Outcome.RUNNING, service.cancel(running.taskId(), null).outcome());
        // 收尾：让 RUNNING 任务正常成功，避免遗留
        assertTrue(store.markSucceeded(running.taskId(), claimed.get(0).getVersion()));
        System.out.println("[PASS] 首次取消/重复取消/已成功/执行中/不存在五种结果均可明确区分");
    }

    @Test
    void resubmitSameKeyAfterCancelRunsAsNewTask() {
        System.out.println("[CASE] 取消后用同一业务键+负载重新提交：作为新任务正常执行一次");
        // 60s 定时提交，确定性地保证取消时任务仍是 PENDING
        SubmitResult first = service.submit("renew-1", "p", 0, Instant.now().plusSeconds(60));
        assertEquals(CancelResult.Outcome.CANCELLED, service.cancel(first.taskId(), null).outcome());

        SubmitResult second = service.submit("renew-1", "p");
        assertEquals(SubmitResult.Outcome.ACCEPTED, second.outcome(), "取消释放幂等键后应能重新提交");
        assertNotEquals(first.taskId(), second.taskId(), "重新提交必须生成新任务");

        TestQueues.await("重新提交的任务执行成功",
                () -> store.findById(second.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(1, executed.size(), "新任务只执行一次，被取消的旧任务不执行");
        assertEquals("renew-1|p", executed.get(0));
        assertEquals(2, store.listAll().size(), "旧 CANCELLED 记录与新 SUCCEEDED 记录都保留可查");
        System.out.println("[PASS] 取消后重新提交得到新 taskId 并正常执行一次，旧记录保留为 CANCELLED");
    }

    @Test
    void resubmitAfterCancelRespectsPerKeyOrder() {
        System.out.println("[CASE] 取消后同键顺序可预期：被取消的队首不占位，同键后续任务先执行，重提任务排队尾");
        // head 60s 后才到点，second 立即可到期但被同键队首挡住；取消 head 后 second 先被调度执行，
        // 再重提 head（新任务）排在 second 之后
        TaskRecord head = newRecord("renew-order", "head", Instant.now().plusSeconds(60));
        TaskRecord second = newRecord("renew-order", "second", Instant.now());
        store.insertIfAbsent(head);
        store.insertIfAbsent(second);

        try {
            Thread.sleep(150); // 远大于轮询间隔：若同键保序失效，second 早就被错误执行
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertTrue(executed.isEmpty(), "队首未到点，同键后续任务必须等待，不得被提前执行");
        assertEquals(CancelResult.Outcome.CANCELLED, store.cancel(head.getId(), null).outcome());

        // 取消后 second 成为同键队首，由调度器正常领取执行
        TestQueues.await("second 在队首取消后执行",
                () -> executed.contains("renew-order|second"));

        // 用同一业务键+负载重新提交 head（立即可执行）：此时 second 已终结，重提任务正常执行
        SubmitResult renewed = service.submit("renew-order", "head");
        TestQueues.await("重提任务执行成功",
                () -> store.findById(renewed.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(2, executed.size());
        assertEquals("renew-order|second", executed.get(0), "同键顺序：second 先执行");
        assertEquals("renew-order|head", executed.get(1), "重提任务在 second 之后执行");
        System.out.println("[PASS] 同键执行顺序=" + executed + "：取消不占位，重提任务在 second 之后，顺序可预期");
    }

    private TaskRecord newRecord(String businessKey, String payload, Instant notBefore) {
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
        t.setPriority(0);
        t.setNotBefore(notBefore);
        return t;
    }
}
