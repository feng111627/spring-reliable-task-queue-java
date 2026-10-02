package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：服务重启恢复。
 * 期望：重启后已成功的任务不被重新执行；PENDING 任务不丢；
 * 崩溃时处于 RUNNING 的任务被回收并重新投递；每个环节的状态可从存储中看出。
 */
class RestartRecoveryTest {

    @TempDir
    Path dir;

    @Test
    void restartRecoversPendingAndRunningButNotSucceeded() throws Exception {
        QueueProperties props = TestQueues.fastProps(dir.toString());

        // ---- 第一次“进程”：完成一个任务，制造一个 RUNNING 残留（模拟崩溃），留一个 PENDING ----
        FileTaskStore store1 = TestQueues.newStore(props);
        TaskQueueService service1 = new TaskQueueService(store1, props);
        TaskDispatcher dispatcher1 = new TaskDispatcher(store1, props);
        List<String> executed1 = new CopyOnWriteArrayList<>();
        dispatcher1.setHandler(task -> executed1.add(task.getBusinessKey()));
        dispatcher1.start();

        System.out.println("[CASE] 重启恢复：第一次运行先成功一个任务");
        service1.submit("finished-1", "p");
        TestQueues.await("finished-1 执行成功",
                () -> store1.findByBusinessKey("finished-1").orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        dispatcher1.stop();

        // 模拟崩溃瞬间：一个任务刚被领取(RUNNING)进程就挂了，另一个还没被调度
        service1.submit("crashed-running-1", "p");
        List<TaskRecord> claimed = store1.claimDue(Instant.now(), 1);
        assertEquals(1, claimed.size());
        assertEquals("crashed-running-1", claimed.get(0).getBusinessKey());
        assertEquals(TaskStatus.RUNNING, store1.findByBusinessKey("crashed-running-1").orElseThrow().getStatus());
        service1.submit("pending-1", "p");
        // 注意：这里不再调用任何 stop/清理，直接丢弃 store1，模拟进程崩溃
        System.out.println("[CASE] 模拟崩溃：crashed-running-1 处于 RUNNING，pending-1 处于 PENDING，进程直接退出");

        // ---- 第二次“进程”：从同一存储目录恢复 ----
        FileTaskStore store2 = TestQueues.newStore(props);
        assertEquals(TaskStatus.SUCCEEDED, store2.findByBusinessKey("finished-1").orElseThrow().getStatus(),
                "重启后已完成任务状态必须保留");
        assertEquals(TaskStatus.RUNNING, store2.findByBusinessKey("crashed-running-1").orElseThrow().getStatus(),
                "崩溃前的 RUNNING 状态应能从存储中看出（便于定位失败环节）");

        TaskQueueService service2 = new TaskQueueService(store2, props);
        TaskDispatcher dispatcher2 = new TaskDispatcher(store2, props);
        List<String> executed2 = new CopyOnWriteArrayList<>();
        dispatcher2.setHandler(task -> executed2.add(task.getBusinessKey()));
        dispatcher2.start(); // 启动时会回收遗留 RUNNING

        TestQueues.await("未完成任务被恢复执行",
                () -> executed2.contains("crashed-running-1") && executed2.contains("pending-1"));
        TestQueues.await("恢复的任务全部成功",
                () -> store2.findByBusinessKey("crashed-running-1").orElseThrow().getStatus() == TaskStatus.SUCCEEDED
                        && store2.findByBusinessKey("pending-1").orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        dispatcher2.stop();

        assertFalse(executed2.contains("finished-1"), "已成功的任务重启后不得被重新执行");
        assertEquals(1, executed2.stream().filter("crashed-running-1"::equals).count(),
                "崩溃残留任务恢复后恰好执行一次");
        assertEquals(1, executed2.stream().filter("pending-1"::equals).count(),
                "PENDING 任务恢复后恰好执行一次");
        assertTrue(Files.exists(Path.of(dir.toString(), "tasks.json")), "本地存储文件应存在");
        System.out.println("[PASS] 重启恢复：finished-1 未重复执行，crashed-running-1 与 pending-1 各恢复执行一次");
    }
}
