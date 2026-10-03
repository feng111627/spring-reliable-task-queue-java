package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：优先级调度与同键保序的交叉。
 * 期望：积压时高优先级任务先被消费；同优先级按提交先后；
 * 但同一业务键内部严格先来后到——后提交的高优先级任务不能插到同键前辈前面。
 * 本测试使用单消费者线程（单条带），使领取顺序可被确定性观察。
 */
class PrioritySchedulingTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;
    private final List<String> executedKeys = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        props.setConsumerThreads(1); // 单条带：执行顺序 == claimDue 的领取顺序
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> {
            executedKeys.add(task.getBusinessKey() + "|" + task.getPayload());
            try {
                Thread.sleep(5); // 放大顺序、拉开时间
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        // 注意：先不启动调度器，先制造积压，再统一开始消费
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void higherPriorityTasksConsumedFirstAcrossKeys() {
        System.out.println("[CASE] 跨键优先级：先积压 3 个默认优先级 + 2 个高优先级任务，再启动消费");
        service.submit("L1", "p", null, null);
        service.submit("L2", "p", null, null);
        service.submit("L3", "p", 0, null);
        service.submit("H1", "p", 10, null);
        service.submit("H2", "p", 10, null);

        dispatcher.start();
        TestQueues.await("5 个任务全部执行完成",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 5);

        System.out.println("[INFO] 实际消费顺序=" + executedKeys);
        List<String> firstTwo = executedKeys.subList(0, 2);
        assertTrue(firstTwo.contains("H1|p") && firstTwo.contains("H2|p"),
                "高优先级任务必须最先被消费，实际前两名=" + firstTwo);
        assertEquals(5, executedKeys.size(), "每个任务恰好执行一次");
        System.out.println("[PASS] 积压场景下两个高优先级任务先于全部默认优先级任务被消费");
    }

    @Test
    void priorityNeverReordersTasksOfSameBusinessKey() {
        System.out.println("[CASE] 同键保序优先于优先级：同键先提交默认任务 A，再提交高优先级任务 B");
        // 同键不同负载：B 优先级再高，也必须排在同键前辈 A 之后
        SubmitResult a = service.submit("key-X", "A", 0, null);
        SubmitResult b = service.submit("key-X", "B", 100, null);
        assertEquals(SubmitResult.Outcome.ACCEPTED, a.outcome());
        assertEquals(SubmitResult.Outcome.ACCEPTED, b.outcome());

        dispatcher.start();
        TestQueues.await("两个任务全部执行完成",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 2);

        System.out.println("[INFO] 实际消费顺序=" + executedKeys);
        assertEquals(List.of("key-X|A", "key-X|B"), executedKeys,
                "同一业务键必须严格按提交先后执行，高优先级不能插队");
        System.out.println("[PASS] 后提交的高优先级任务 B 没有越过同键前辈 A，同键顺序保持 A -> B");
    }
}
