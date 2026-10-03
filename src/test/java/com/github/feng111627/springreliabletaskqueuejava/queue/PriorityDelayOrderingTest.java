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
 * 场景：优先级、定时投递与同键保序三者交叉。
 * 构造（全部先积压再消费，单消费者以便确定顺序）：
 *   key-A: a1(默认, 延迟 300ms)  a2(默认, 立即)     —— a1 未到点时 a2 不得提前；
 *   key-B: b1(高优 10, 延迟 600ms)                   —— 优先级再高，没到点也不能执行；
 *   key-C: c1(默认, 立即)                            —— 立即可执行；
 *   key-D: d1(高优 10, 立即)                         —— 到点的高优先级应最先。
 * 期望开始时刻：d1 最先（高优且到点）；随后 c1（到点的默认优先级）；
 * 300ms 后 a1 -> a2（同键严格序，a2 不能越过延迟的 a1）；600ms 后 b1。
 */
class PriorityDelayOrderingTest {

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
    void priorityAndDelayAndPerKeyFifoInteractCorrectly() {
        System.out.println("[CASE] 优先级 x 定时 x 同键保序交叉：d1(高优立即) c1(默认立即) "
                + "a1(默认延迟300ms) a2(同键立即) b1(高优延迟600ms)");
        Instant start = Instant.now();
        service.submit("key-A", "a1", 0, start.plusMillis(300));
        service.submit("key-A", "a2", 0, null);
        service.submit("key-B", "b1", 10, start.plusMillis(600));
        service.submit("key-C", "c1", 0, null);
        service.submit("key-D", "d1", 10, null);

        dispatcher.start();
        TestQueues.await("5 个任务全部成功",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 5);

        List<String> payloads = executed.stream().map(x -> x.substring(x.indexOf('|') + 1)).toList();
        System.out.println("[INFO] 实际消费顺序=" + executed);
        assertEquals(List.of("d1", "c1", "a1", "a2", "b1"), payloads,
                "高优只在到点任务间生效，同键严格 FIFO 未被打乱");
        System.out.println("[PASS] 顺序 d1 -> c1 -> a1 -> a2 -> b1：高优只在到点任务间生效，同键严格 FIFO 未被打乱");
    }
}
