package com.github.feng111627.springreliabletaskqueuejava.tests.taskqueue;

import com.github.feng111627.springreliabletaskqueuejava.taskqueue.FileTaskStore;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.SubmitResult;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.Task;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskDispatcher;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskQueueProperties;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskQueueService;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用例：失败重试与死信。
 * 失败按指数退避重试，耗尽后进入死信；死信可查询、可再投递；
 * 重试中的任务不得被重复投递、重复计数。
 */
class RetryAndDeadLetterTest {

    @TempDir
    Path tempDir;

    private TaskQueueProperties properties;
    private FileTaskStore store;
    private TaskQueueService service;
    private TaskDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        properties = new TaskQueueProperties();
        properties.setStoragePath(tempDir.resolve("queue.json").toString());
        properties.setMaxAttempts(3);
        properties.setBackoffBaseMs(50);
        properties.setMaxBackoffMs(200);
        properties.setPollIntervalMs(20);
        properties.setConsumerCount(2);
        store = new FileTaskStore(properties);
        store.recover();
        service = new TaskQueueService(store, properties);
    }

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.stop();
        }
    }

    @Test
    void retriesExhaustedThenGoesToDeadLetter() {
        System.out.println("[用例] 重试耗尽：任务持续失败，maxAttempts=3");
        AtomicInteger executions = new AtomicInteger();
        dispatcher = new TaskDispatcher(store, task -> {
            int n = executions.incrementAndGet();
            System.out.println("[执行] 第 " + n + " 次尝试, 任务=" + task.getId());
            throw new RuntimeException("模拟业务失败 #" + n);
        }, properties);
        dispatcher.start();

        SubmitResult result = service.submit("always-fails", "p");
        String taskId = result.getTask().getId();

        TestSupport.await("任务进入死信", () -> service.getTask(taskId).getStatus() == TaskStatus.DEAD);
        Task dead = service.getTask(taskId);
        System.out.println("[结果] 状态=" + dead.getStatus() + ", attempts=" + dead.getAttempts()
                + ", lastError=" + dead.getLastError());
        assertEquals(3, dead.getAttempts());
        assertEquals(3, executions.get(), "执行次数必须等于 maxAttempts，不得重复投递");
        assertTrue(dead.getLastError().contains("模拟业务失败"));

        List<Task> deadLetters = service.listDeadLetters();
        assertEquals(1, deadLetters.size());
        assertEquals(taskId, deadLetters.get(0).getId());
        System.out.println("[通过] 重试 3 次后进入死信，死信可查询，无重复投递");
    }

    @Test
    void deadLetterCanBeRedeliveredAndSucceed() {
        System.out.println("[用例] 死信再投递：先失败进死信，再投递后成功");
        AtomicInteger executions = new AtomicInteger();
        dispatcher = new TaskDispatcher(store, task -> {
            int n = executions.incrementAndGet();
            if (n <= 3) {
                throw new RuntimeException("前 3 次失败");
            }
            System.out.println("[执行] 第 " + n + " 次尝试成功");
        }, properties);
        dispatcher.start();

        String taskId = service.submit("flaky", "p").getTask().getId();
        TestSupport.await("任务进入死信", () -> service.getTask(taskId).getStatus() == TaskStatus.DEAD);
        assertEquals(3, executions.get());

        Task redelivered = service.redeliver(taskId);
        System.out.println("[结果] 再投递后状态=" + redelivered.getStatus() + ", attempts 重置=" + redelivered.getAttempts());
        assertEquals(TaskStatus.PENDING, redelivered.getStatus());
        assertEquals(0, redelivered.getAttempts());

        TestSupport.await("再投递后执行成功", () -> service.getTask(taskId).getStatus() == TaskStatus.SUCCEEDED);
        assertEquals(4, executions.get());
        assertTrue(service.listDeadLetters().isEmpty());
        System.out.println("[通过] 死信再投递后成功，死信列表清空");
    }

    @Test
    void retryingTaskIsNotDeliveredTwice() {
        System.out.println("[用例] 重试中不得重复投递：慢任务 + 多消费者");
        ConcurrentHashMap<String, AtomicInteger> executionsPerTask = new ConcurrentHashMap<>();
        dispatcher = new TaskDispatcher(store, task -> {
            executionsPerTask.computeIfAbsent(task.getId(), k -> new AtomicInteger()).incrementAndGet();
            Thread.sleep(150); // 慢任务，拉长 RUNNING 窗口
            throw new RuntimeException("失败以触发重试");
        }, properties);
        dispatcher.start();

        String taskId = service.submit("slow-failing", "p").getTask().getId();
        TestSupport.await("任务进入死信", Duration.ofSeconds(15),
                () -> service.getTask(taskId).getStatus() == TaskStatus.DEAD);

        int total = executionsPerTask.get(taskId).get();
        System.out.println("[结果] 总执行次数=" + total + " (期望=3)");
        assertEquals(3, total, "同一任务在重试期间不得被多个消费者重复领取");
        System.out.println("[通过] 重试中的任务未被重复投递、重复计数");
    }

    @Test
    void succeededTaskIsNotRedelivered() {
        System.out.println("[用例] 成功任务不得重复投递");
        AtomicInteger executions = new AtomicInteger();
        dispatcher = new TaskDispatcher(store, task -> executions.incrementAndGet(), properties);
        dispatcher.start();

        String taskId = service.submit("success-once", "p").getTask().getId();
        TestSupport.await("任务成功", () -> service.getTask(taskId).getStatus() == TaskStatus.SUCCEEDED);
        // 再观察一段时间，确认不会被再次领取
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("[结果] 执行次数=" + executions.get());
        assertEquals(1, executions.get());
        System.out.println("[通过] 成功任务只执行一次");
    }
}
