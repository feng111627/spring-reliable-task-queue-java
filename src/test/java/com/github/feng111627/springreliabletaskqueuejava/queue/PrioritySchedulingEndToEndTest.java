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
 * 场景（端到端，真实轮询调度）：积压时高优先级任务先被消费。
 * 单独成类，避免与无后台线程的存储层确定性用例共享 store 产生调度竞态。
 */
class PrioritySchedulingEndToEndTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;
    private final List<String> finished = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        store = TestQueues.newStore(props);
        service = new TaskQueueService(store, props);
        dispatcher = new TaskDispatcher(store, props);
    }

    @AfterEach
    void tearDown() {
        dispatcher.stop();
    }

    @Test
    void higherPriorityServedFirstWhenBacklogExists() throws Exception {
        System.out.println("[CASE] 端到端：积压时高优先级任务先完成（真实轮询调度）");
        // 处理器对低优先级任务阻塞，人工制造“占满线程的积压”后再放行
        Object gate = new Object();
        final boolean[] released = {false};
        dispatcher.setHandler(task -> {
            if (task.getPriority() == 0) {
                synchronized (gate) {
                    while (!released[0]) {
                        gate.wait();
                    }
                }
            }
            finished.add(task.getBusinessKey());
        });
        dispatcher.start();

        // 4 个低优先级、业务键各异的任务占满 4 个消费者条带
        for (int i = 0; i < 4; i++) {
            service.submit("low-" + i, "p", 0, null);
        }
        Thread.sleep(200); // 等首批低优先级全部开始执行（处于阻塞）
        assertEquals(4, store.listByStatus(TaskStatus.RUNNING).size(), "前置条件：4 个条带均被低优先级占满");

        // 再提交 4 个高优先级任务（业务键互不相同，且与低优先级键不同），随后立刻放行
        for (int i = 0; i < 4; i++) {
            service.submit("high-" + i, "p", 10, null);
        }
        Thread.sleep(150); // 高优先级在条带队列里排队等待（低优先级仍阻塞）
        synchronized (gate) {
            released[0] = true;
            gate.notifyAll();
        }

        TestQueues.await("全部 8 个任务完成", () -> finished.size() == 8);
        // 放行时 4 个低优先级仍在运行（位置 0..3），高优先级 4 个都在排队；
        // 它们必须全部先于“放行后才完成的低优先级”结束
        int lastHigh = -1;
        int firstLowAfterGate = Integer.MAX_VALUE;
        for (int i = 0; i < finished.size(); i++) {
            if (finished.get(i).startsWith("high-")) {
                lastHigh = i;
            } else if (i > 3) {
                firstLowAfterGate = Math.min(firstLowAfterGate, i);
            }
        }
        assertTrue(lastHigh < firstLowAfterGate,
                "高优先级应先于放行时仍在排队的低优先级完成，实际完成顺序=" + finished);
        System.out.println("[PASS] 放行后完成顺序=" + finished + "，4 个高优先级全部先于积压低优先级完成");
    }
}
