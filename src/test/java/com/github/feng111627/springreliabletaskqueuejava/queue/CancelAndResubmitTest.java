package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：取消与重新提交。
 * 覆盖：
 * 1) 排队中(PENDING)取消：取消后不再执行，结果可查询，重复取消给出可区分结果；
 * 2) 死信(DEAD)与退避中(FAILED)可取消；执行中(RUNNING)给出 NOT_CANCELLABLE；成功任务不可取消；
 * 3) 取消后用同一业务键（甚至同负载）可重新提交并正常执行，且同键顺序可预期；
 * 4) 重新提交的新任务若带延迟，仍然排在已取消前辈之后的语义不受影响（取消是终态不占队头）。
 */
class CancelAndResubmitTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;
    private final List<String> executed = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        props.setConsumerThreads(1);
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> executed.add(task.getBusinessKey() + "|" + task.getPayload()));
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void cancelPendingTaskStopsExecutionAndRepeatedCancelIsDistinguishable() throws Exception {
        System.out.println("[CASE] 取消排队中任务：提交一个延迟 1h 的任务并取消，结果可查询");
        SubmitResult r = service.submit("cancel-1", "p", null, Instant.now().plusSeconds(3600));
        String id = r.taskId();

        CancelResult first = service.cancel(id);
        assertEquals(CancelResult.Outcome.CANCELLED, first.outcome());
        TaskRecord cancelled = store.findById(id).orElseThrow();
        assertEquals(TaskStatus.CANCELLED, cancelled.getStatus(), "取消结果必须持久化、可查询");

        CancelResult second = service.cancel(id);
        assertEquals(CancelResult.Outcome.ALREADY_CANCELLED, second.outcome(),
                "重复取消必须给出与首次取消可区分的结果");
        assertNotEquals(first.outcome(), second.outcome());

        CancelResult missing = service.cancel("not-exist-id");
        assertEquals(CancelResult.Outcome.NOT_FOUND, missing.outcome());

        dispatcher.start();
        Thread.sleep(400);
        assertTrue(executed.isEmpty(), "取消后的任务绝不能再被执行");
        dispatcher.stop();
        System.out.println("[PASS] 排队任务取消后不再执行；首次=CANCELLED，重复=ALREADY_CANCELLED，不存在=NOT_FOUND");
    }

    @Test
    void cancelDeadAndFailedAndRunningHaveExpectedOutcomes() throws Exception {
        System.out.println("[CASE] 死信/退避中可取消，执行中不可取消，成功终态不可取消");
        // 构造一个死信：处理器先让所有任务失败
        dispatcher.stop();
        TaskDispatcher failing = new TaskDispatcher(store, props);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        failing.setHandler(task -> {
            calls.incrementAndGet();
            throw new IllegalStateException("总是失败");
        });
        failing.start();
        SubmitResult dead = service.submit("cancel-dead", "p");
        TestQueues.await("任务进入死信",
                () -> store.findById(dead.taskId()).orElseThrow().getStatus() == TaskStatus.DEAD);

        CancelResult cancelDead = service.cancel(dead.taskId());
        assertEquals(CancelResult.Outcome.CANCELLED, cancelDead.outcome());
        assertEquals(TaskStatus.CANCELLED, store.findById(dead.taskId()).orElseThrow().getStatus());
        System.out.println("[INFO] 死信任务取消成功");

        // 构造一个 FAILED（退避中）任务并取消：maxAttempts 给大一点，停在第一次失败后的 FAILED
        failing.stop();
        props.setMaxAttempts(5);
        SubmitResult retrying = service.submit("cancel-failed", "p");
        TaskDispatcher failing2 = new TaskDispatcher(store, props);
        failing2.setHandler(task -> {
            throw new IllegalStateException("失败一次进入退避");
        });
        failing2.start();
        TestQueues.await("任务进入 FAILED 退避中",
                () -> store.findById(retrying.taskId()).orElseThrow().getStatus() == TaskStatus.FAILED);
        CancelResult cancelFailed = service.cancel(retrying.taskId());
        assertEquals(CancelResult.Outcome.CANCELLED, cancelFailed.outcome(), "退避重试中的任务应可取消");
        failing2.stop();

        // 成功任务不可取消
        TaskDispatcher ok = new TaskDispatcher(store, props);
        ok.setHandler(task -> executed.add(task.getBusinessKey()));
        ok.start();
        SubmitResult done = service.submit("cancel-done", "p");
        TestQueues.await("任务成功",
                () -> store.findById(done.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        CancelResult cancelDone = service.cancel(done.taskId());
        assertEquals(CancelResult.Outcome.ALREADY_SUCCEEDED, cancelDone.outcome(), "已成功任务不可取消");
        ok.stop();

        // RUNNING 不可取消：直接用存储层把一个任务置为 RUNNING 来稳定构造
        SubmitResult running = service.submit("cancel-running", "p", null, Instant.now().plusSeconds(3600));
        store.claimDue(Instant.now().plusSeconds(7200), 10);
        assertEquals(TaskStatus.RUNNING, store.findById(running.taskId()).orElseThrow().getStatus());
        CancelResult cancelRunning = service.cancel(running.taskId());
        assertEquals(CancelResult.Outcome.NOT_CANCELLABLE, cancelRunning.outcome());
        assertEquals(TaskStatus.RUNNING, cancelRunning.currentStatus());
        System.out.println("[PASS] DEAD/FAILED 可取消，SUCCEEDED=ALREADY_SUCCEEDED，RUNNING=NOT_CANCELLABLE");
    }

    @Test
    void sameKeyAndPayloadCanBeResubmittedAfterCancelAndRuns() throws Exception {
        System.out.println("[CASE] 取消后同业务键+同负载重新提交：作为新任务正常执行");
        SubmitResult first = service.submit("order-r", "same-payload", null, Instant.now().plusSeconds(3600));
        service.cancel(first.taskId());

        // 同键同负载：旧任务已取消（终态），新提交必须 ACCEPTED 为新任务，而不是 DUPLICATE
        SubmitResult again = service.submit("order-r", "same-payload");
        assertEquals(SubmitResult.Outcome.ACCEPTED, again.outcome(), "取消后同键同负载应能重新提交");
        assertNotEquals(first.taskId(), again.taskId(), "重新提交应生成新任务 id");

        dispatcher.start();
        TestQueues.await("新任务执行成功",
                () -> store.findById(again.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(List.of("order-r|same-payload"), executed, "只有重新提交的新任务执行了一次");
        assertEquals(TaskStatus.CANCELLED, store.findById(first.taskId()).orElseThrow().getStatus(),
                "旧任务记录保留为 CANCELLED 可追溯");
        dispatcher.stop();
        System.out.println("[PASS] 取消后同键同负载重新提交被接受为新任务并执行，旧记录保留为 CANCELLED");
    }

    @Test
    void resubmittedTaskOrderingOnSameKeyIsPredictable() throws Exception {
        System.out.println("[CASE] 取消后同键顺序可预期：取消队头后，新任务与未取消的后辈按序执行");
        // 同键三个任务：A(延迟, 后被取消) B(立即可执行) C(立即可执行)。A 取消后 B、C 按提交先后执行。
        SubmitResult a = service.submit("key-ord", "A", null, Instant.now().plusSeconds(3600));
        SubmitResult b = service.submit("key-ord", "B", null, null);
        SubmitResult c = service.submit("key-ord", "C", null, null);
        assertEquals(CancelResult.Outcome.CANCELLED, service.cancel(a.taskId()).outcome());

        dispatcher.start();
        TestQueues.await("B、C 全部成功", () ->
                store.findById(b.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED
                        && store.findById(c.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(List.of("key-ord|B", "key-ord|C"), executed,
                "已取消的 A 是终态不占队头，未取消的 B、C 仍严格按提交先后执行");

        // A 取消后再用同键提交 D：D 排在现存活动任务之后；此时 B、C 已终态，D 直接执行
        SubmitResult d = service.submit("key-ord", "D", null, null);
        TestQueues.await("D 执行成功",
                () -> store.findById(d.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(List.of("key-ord|B", "key-ord|C", "key-ord|D"), executed,
                "取消后重新提交的新任务接在队尾，顺序为 B -> C -> D");
        System.out.println("[PASS] 同键顺序可预期：取消 A 后 B->C，再提交 D 接队尾 B->C->D");
    }
}
