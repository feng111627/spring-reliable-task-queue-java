package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：重复提交（幂等键 = 业务键 + 负载）。
 * 期望：同一任务的重复提交被去重、只执行一次，且返回可区分的结果与原因；
 * 同业务键不同负载是不同任务，正常入队执行。
 */
class DuplicateSubmissionTest {

    @TempDir
    Path dir;

    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;
    private final ConcurrentHashMap<String, AtomicInteger> executions = new ConcurrentHashMap<>();
    private final List<String> executedOrder = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        QueueProperties props = TestQueues.fastProps(dir.toString());
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> {
            executions.computeIfAbsent(task.getBusinessKey() + "|" + task.getPayload(),
                    k -> new AtomicInteger()).incrementAndGet();
            executedOrder.add(task.getBusinessKey() + "|" + task.getPayload());
        });
        dispatcher.start();
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void duplicateSubmitIsDedupedAndExecutedOnlyOnce() {
        System.out.println("[CASE] 重复提交去重：相同业务键+相同负载连续提交 3 次");
        SubmitResult first = service.submit("order-1", "payload-1");
        SubmitResult second = service.submit("order-1", "payload-1");
        SubmitResult third = service.submit("order-1", "payload-1");

        assertEquals(SubmitResult.Outcome.ACCEPTED, first.outcome());
        assertEquals(SubmitResult.Outcome.DUPLICATE, second.outcome());
        assertEquals(SubmitResult.Outcome.DUPLICATE, third.outcome());
        assertEquals(first.taskId(), second.taskId(), "重复提交应指向同一个已存在任务");
        assertTrue(second.reason().contains("去重"), "重复提交必须给出可区分的原因");

        TestQueues.await("任务执行完成",
                () -> store.findById(first.taskId()).orElseThrow().getStatus() == TaskStatus.SUCCEEDED);

        // 成功之后再提交同一任务，仍然是 DUPLICATE，且能看到已有任务已成功
        SubmitResult afterSuccess = service.submit("order-1", "payload-1");
        assertEquals(SubmitResult.Outcome.DUPLICATE, afterSuccess.outcome());
        assertEquals(TaskStatus.SUCCEEDED, afterSuccess.currentStatus());

        assertEquals(1, executions.get("order-1|payload-1").get(), "同一任务只能真正执行一次");
        assertEquals(1, store.listAll().size(), "队列中只应存在一条任务记录");
        System.out.println("[PASS] 重复提交被去重，任务仅执行一次，重复提交返回了可区分的原因与当前状态");
    }

    @Test
    void sameKeyDifferentPayloadAreDistinctTasks() {
        System.out.println("[CASE] 同业务键不同负载：应作为不同任务各自入队执行");
        SubmitResult a = service.submit("order-2", "created");
        SubmitResult b = service.submit("order-2", "paid");
        SubmitResult dup = service.submit("order-2", "created");

        assertEquals(SubmitResult.Outcome.ACCEPTED, a.outcome());
        assertEquals(SubmitResult.Outcome.ACCEPTED, b.outcome());
        assertEquals(SubmitResult.Outcome.DUPLICATE, dup.outcome());
        assertEquals(a.taskId(), dup.taskId());

        TestQueues.await("两个任务都执行完成",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 2);
        assertEquals(1, executions.get("order-2|created").get());
        assertEquals(1, executions.get("order-2|paid").get());
        assertEquals(2, store.listAll().size());
        System.out.println("[PASS] 同键不同负载各自执行一次，执行顺序=" + executedOrder);
    }
}
