package com.github.feng111627.springreliabletaskqueuejava.tests.taskqueue;

import com.github.feng111627.springreliabletaskqueuejava.taskqueue.FileTaskStore;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.Task;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskDispatcher;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskQueueProperties;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskQueueService;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 用例：重启恢复。
 * 进程重启后：未完成任务不丢（PENDING 继续、RUNNING 重置为 PENDING 重投），
 * 已完成任务（SUCCEEDED/DEAD）不被重新执行。
 */
class RestartRecoveryTest {

    @TempDir
    Path tempDir;

    private TaskQueueProperties newProperties() {
        TaskQueueProperties properties = new TaskQueueProperties();
        properties.setStoragePath(tempDir.resolve("queue.json").toString());
        properties.setConsumerCount(2);
        properties.setPollIntervalMs(10);
        properties.setBackoffBaseMs(50);
        properties.setMaxBackoffMs(200);
        return properties;
    }

    @Test
    void storeRecoverResetsRunningAndKeepsTerminalStates() {
        System.out.println("[用例] 存储层重启恢复：RUNNING 重置为 PENDING，终态保持不变");
        TaskQueueProperties properties = newProperties();
        FileTaskStore store1 = new FileTaskStore(properties);
        store1.recover();

        Task pending = task("k-pending", TaskStatus.PENDING);
        Task running = task("k-running", TaskStatus.RUNNING);
        Task succeeded = task("k-succeeded", TaskStatus.SUCCEEDED);
        Task dead = task("k-dead", TaskStatus.DEAD);
        store1.insert(pending);
        store1.insert(running);
        store1.insert(succeeded);
        store1.insert(dead);

        // 模拟重启：用同一存储文件创建新实例并恢复
        FileTaskStore store2 = new FileTaskStore(properties);
        store2.recover();

        Map<String, TaskStatus> statuses = store2.listAll().stream()
                .collect(Collectors.toMap(Task::getBusinessKey, Task::getStatus));
        System.out.println("[结果] 恢复后状态=" + statuses);
        assertEquals(TaskStatus.PENDING, statuses.get("k-pending"), "PENDING 应保持待投递");
        assertEquals(TaskStatus.PENDING, statuses.get("k-running"), "中断的 RUNNING 应重置为 PENDING 重新投递");
        assertEquals(TaskStatus.SUCCEEDED, statuses.get("k-succeeded"), "已成功任务不得被重置");
        assertEquals(TaskStatus.DEAD, statuses.get("k-dead"), "死信任务不得被重置");
        System.out.println("[通过] 重启后各状态恢复正确");
    }

    @Test
    void unfinishedTaskSurvivesRestartAndSucceededIsNotReexecuted() {
        System.out.println("[用例] 端到端重启恢复：执行中崩溃的任务重启后重跑，已成功的不重跑");
        TaskQueueProperties properties = newProperties();

        // ---- 第一次“进程”：done-task 成功；stuck-task 被领取为 RUNNING 后进程崩溃 ----
        FileTaskStore store1 = new FileTaskStore(properties);
        store1.recover();
        TaskQueueService service1 = new TaskQueueService(store1, properties);
        AtomicInteger firstRoundExecutions = new AtomicInteger();
        TaskDispatcher dispatcher1 = new TaskDispatcher(store1,
                task -> firstRoundExecutions.incrementAndGet(), properties);
        dispatcher1.start();

        String doneId = service1.submit("done-task", "p").getTask().getId();
        TestSupport.await("done-task 成功", () -> service1.getTask(doneId).getStatus() == TaskStatus.SUCCEEDED);
        dispatcher1.stop();

        // 模拟消费者领取 stuck-task 后进程被 kill -9：状态停留在 RUNNING 并已落盘
        String stuckId = service1.submit("stuck-task", "p").getTask().getId();
        var claimed = store1.claimNextDue(java.util.Set.of());
        org.junit.jupiter.api.Assertions.assertTrue(claimed.isPresent());
        org.junit.jupiter.api.Assertions.assertEquals(stuckId, claimed.get().getId());
        System.out.println("[场景] 进程崩溃, stuck-task 落盘状态="
                + store1.findById(stuckId).map(Task::getStatus).orElseThrow());

        // ---- 第二次“进程”：同一存储文件重启 ----
        FileTaskStore store2 = new FileTaskStore(properties);
        store2.recover();
        TaskQueueService service2 = new TaskQueueService(store2, properties);
        System.out.println("[场景] 重启恢复后, stuck-task 状态="
                + service2.getTask(stuckId).getStatus() + ", done-task 状态=" + service2.getTask(doneId).getStatus());
        Map<String, AtomicInteger> secondRoundExecutions = new ConcurrentHashMap<>();
        TaskDispatcher dispatcher2 = new TaskDispatcher(store2, task ->
                secondRoundExecutions.computeIfAbsent(task.getBusinessKey(), k -> new AtomicInteger()).incrementAndGet(),
                properties);
        dispatcher2.start();

        TestSupport.await("stuck-task 重启后执行成功", Duration.ofSeconds(15),
                () -> service2.getTask(stuckId).getStatus() == TaskStatus.SUCCEEDED);
        dispatcher2.stop();

        System.out.println("[结果] 重启后各任务执行次数=" + secondRoundExecutions);
        assertEquals(1, secondRoundExecutions.get("stuck-task").get(), "未完成任务重启后应被重新执行一次");
        assertEquals(null, secondRoundExecutions.get("done-task"), "已成功任务重启后不得被重新执行");
        System.out.println("[通过] 未完成任务不丢、已完成任务不重跑");
    }

    private Task task(String key, TaskStatus status) {
        Task t = new Task();
        t.setId(UUID.randomUUID().toString());
        t.setBusinessKey(key);
        t.setPayload("p");
        t.setStatus(status);
        t.setMaxAttempts(3);
        t.setNextAttemptAtEpochMs(System.currentTimeMillis());
        t.setCreatedAt(Instant.now());
        t.setUpdatedAt(Instant.now());
        return t;
    }
}
