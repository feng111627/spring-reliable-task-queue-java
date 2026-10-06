package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景：带旧版本数据的升级兼容（回归测试）。
 * 旧版本（无优先级/定时/取消）留下的 tasks.json 中，记录没有 seq/priority/notBefore 字段。
 * 期望：直接用旧数据启动，无需手工迁移；
 * 同一业务键内旧任务与新任务严格按提交先后执行（新任务优先级再高也不能越过未终结的旧任务）；
 * 崩溃重启后该顺序不变；已终结（成功/取消/死信）的旧任务不再阻塞同键后继；
 * 跨业务键的优先级调度保持原样。
 * 本测试使用单消费者线程（单条带），使执行顺序可被确定性观察。
 */
class LegacyDataOrderingTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private final List<String> executed = new CopyOnWriteArrayList<>();

    private QueueProperties props() {
        if (props == null) {
            props = TestQueues.fastProps(dir.toString());
            props.setConsumerThreads(1); // 单条带：执行顺序 == 领取顺序
        }
        return props;
    }

    private TaskDispatcher newDispatcher(FileTaskStore store) {
        TaskDispatcher dispatcher = new TaskDispatcher(store, props());
        dispatcher.setHandler(task -> executed.add(task.getBusinessKey() + "|" + task.getPayload()));
        return dispatcher;
    }

    /**
     * 手写一条旧版本格式的任务记录 JSON：只有旧版本的字段
     * （id/businessKey/payload/status/attempts/maxAttempts/nextAttemptAt/createdAt/updatedAt/lastError/version），
     * 不含新版本引入的 priority/seq/notBefore。
     */
    private static String legacyRecord(String id, String businessKey, String payload, String status,
                                       int attempts, String nextAttemptAt, String createdAt, String lastError) {
        return """
                {"id":"%s","businessKey":"%s","payload":"%s","status":"%s","attempts":%d,"maxAttempts":3,\
                "nextAttemptAt":"%s","createdAt":"%s","updatedAt":"%s","lastError":%s,"version":1}"""
                .formatted(id, businessKey, payload, status, attempts, nextAttemptAt, createdAt, createdAt,
                        lastError == null ? "null" : "\"" + lastError + "\"");
    }

    private void writeLegacyStore(String... records) throws Exception {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("tasks.json"), "[" + String.join(",", records) + "]");
    }

    @Test
    void legacyPendingAndRetryingTasksBlockNewHighPrioritySameKey() throws Exception {
        System.out.println("[CASE] 旧数据+同键高优新任务：旧任务 PENDING/退避重试中，新任务 priority=100，旧任务必须先执行");
        // 旧版本数据：key-A 有一条排队中的旧任务；key-B 有一条失败退避、等待重试的旧任务
        writeLegacyStore(
                legacyRecord("legacy-a", "key-A", "old-pending", "PENDING", 0,
                        "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", null),
                legacyRecord("legacy-b", "key-B", "old-retrying", "FAILED", 1,
                        "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z", "boom"));

        FileTaskStore store = TestQueues.newStore(props());
        TaskQueueService service = new TaskQueueService(store, props());

        // 升级加载后：旧记录被回填同键序号 seq=1，且文件被自动升级（无需手工迁移）
        assertEquals(1, store.findById("legacy-a").orElseThrow().getSeq(), "旧记录应被回填 seq=1");
        assertEquals(1, store.findById("legacy-b").orElseThrow().getSeq(), "旧记录应被回填 seq=1");
        assertTrue(Files.readString(dir.resolve("tasks.json")).contains("\"seq\":1"),
                "回填后应立即落盘，旧数据文件被自动升级");

        // 同业务键提交高优先级新任务（调度器尚未启动，先制造积压）
        service.submit("key-A", "new-high", 100, null);
        service.submit("key-B", "new-high", 100, null);
        assertEquals(2, store.findByBusinessKey("key-A").orElseThrow().getSeq(), "新任务应接在旧任务序号之后");

        TaskDispatcher dispatcher = newDispatcher(store);
        dispatcher.start();
        TestQueues.await("4 个任务全部执行成功",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 4);
        dispatcher.stop();

        System.out.println("[INFO] 实际消费顺序=" + executed);
        assertTrue(executed.indexOf("key-A|old-pending") < executed.indexOf("key-A|new-high"),
                "key-A：排队中的旧任务必须先于高优新任务执行，实际=" + executed);
        assertTrue(executed.indexOf("key-B|old-retrying") < executed.indexOf("key-B|new-high"),
                "key-B：退避重试中的旧任务必须先于高优新任务执行，实际=" + executed);
        assertEquals(4, executed.size(), "每个任务恰好执行一次");
        System.out.println("[PASS] 旧任务(排队/退避重试)未被同键高优新任务越过，同键严格按提交先后执行");
    }

    @Test
    void restartAfterCrashKeepsLegacyBeforeNewOrder() throws Exception {
        System.out.println("[CASE] 重启恢复：旧任务崩溃时处于 RUNNING，恢复后仍先于同键高优新任务执行");
        writeLegacyStore(
                legacyRecord("legacy-r", "key-R", "old", "PENDING", 0,
                        "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", null));

        // ---- 第一次“进程”：用旧数据启动，提交同键高优新任务，随后模拟崩溃 ----
        FileTaskStore store1 = TestQueues.newStore(props());
        TaskQueueService service1 = new TaskQueueService(store1, props());
        service1.submit("key-R", "new-high", 100, null);
        // 模拟崩溃瞬间：旧任务刚被领取(RUNNING)进程就挂了
        List<TaskRecord> claimed = store1.claimDue(java.time.Instant.now(), 10);
        assertEquals(1, claimed.size());
        assertEquals("legacy-r", claimed.get(0).getId(),
                "旧任务是同键队头，必须先被领取（高优新任务被它挡住）");
        assertEquals(TaskStatus.RUNNING, store1.findById("legacy-r").orElseThrow().getStatus());
        System.out.println("[CASE] 模拟崩溃：legacy-r 处于 RUNNING，new-high 排队中，进程直接退出");
        // 不做任何清理，直接丢弃 store1

        // ---- 第二次“进程”：从同一存储目录恢复 ----
        FileTaskStore store2 = TestQueues.newStore(props());
        // 序号在第一次启动时已回填并落盘，重启后保持不变
        assertEquals(1, store2.findById("legacy-r").orElseThrow().getSeq(), "重启后旧任务序号保持 1");
        assertEquals(2, store2.findByBusinessKey("key-R").orElseThrow().getSeq(), "重启后新任务序号保持 2");

        TaskDispatcher dispatcher2 = newDispatcher(store2);
        dispatcher2.start(); // 启动时回收遗留 RUNNING 的 legacy-r
        TestQueues.await("两个任务都恢复执行成功",
                () -> store2.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 2);
        dispatcher2.stop();

        System.out.println("[INFO] 重启后实际消费顺序=" + executed);
        assertEquals(List.of("key-R|old", "key-R|new-high"), executed,
                "崩溃重启后旧任务与新任务的相对先后不能变");
        System.out.println("[PASS] 重启回收 RUNNING 旧任务后，仍按 旧 -> 新 顺序执行，未按优先级乱跑");
    }

    @Test
    void terminalLegacyTasksDoNotBlockSameKeySuccessors() throws Exception {
        System.out.println("[CASE] 终态旧任务不阻塞：旧任务已成功/已死信，同键高优新任务应直接执行");
        writeLegacyStore(
                legacyRecord("legacy-ok", "key-DONE", "old-succeeded", "SUCCEEDED", 1,
                        "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", null),
                legacyRecord("legacy-dead", "key-DEAD", "old-dead", "DEAD", 3,
                        "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z", "gave up"));

        FileTaskStore store = TestQueues.newStore(props());
        TaskQueueService service = new TaskQueueService(store, props());
        service.submit("key-DONE", "new-high", 100, null);
        service.submit("key-DEAD", "new-high", 100, null);

        TaskDispatcher dispatcher = newDispatcher(store);
        dispatcher.start();
        // 若终态旧任务仍占着队头，新任务将永远等不到执行，下面的等待会超时失败
        TestQueues.await("两个新任务都执行成功",
                () -> store.findByBusinessKey("key-DONE").orElseThrow().getStatus() == TaskStatus.SUCCEEDED
                        && store.findByBusinessKey("key-DEAD").orElseThrow().getStatus() == TaskStatus.SUCCEEDED);
        dispatcher.stop();

        System.out.println("[INFO] 实际消费顺序=" + executed);
        assertEquals(2, executed.size(), "终态旧任务不得被重新执行");
        assertFalse(executed.contains("key-DONE|old-succeeded"), "已成功的旧任务不得重复执行");
        assertFalse(executed.contains("key-DEAD|old-dead"), "死信旧任务不得被投递");
        assertEquals(TaskStatus.DEAD, store.findById("legacy-dead").orElseThrow().getStatus(),
                "旧死信任务状态保持终态不变");
        System.out.println("[PASS] 已成功/已死信的旧任务不再阻塞同键后继，新任务直接执行且旧任务未被重放");
    }

    @Test
    void crossKeyPrioritySchedulingUnchangedWithLegacyData() throws Exception {
        System.out.println("[CASE] 跨键优先级保持：旧数据(默认优先级)积压时，别的高优键任务仍先被消费");
        writeLegacyStore(
                legacyRecord("legacy-l1", "key-L1", "old-low", "PENDING", 0,
                        "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", null),
                legacyRecord("legacy-l2", "key-L2", "old-low", "PENDING", 0,
                        "2026-01-01T00:00:00Z", "2026-01-01T00:00:01Z", null));

        FileTaskStore store = TestQueues.newStore(props());
        TaskQueueService service = new TaskQueueService(store, props());
        service.submit("key-H", "new-high", 10, null); // 不同业务键的高优新任务

        TaskDispatcher dispatcher = newDispatcher(store);
        dispatcher.start();
        TestQueues.await("3 个任务全部执行成功",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 3);
        dispatcher.stop();

        System.out.println("[INFO] 实际消费顺序=" + executed);
        assertEquals("key-H|new-high", executed.get(0),
                "跨业务键时高优先级仍最先被消费，实际顺序=" + executed);
        assertEquals(3, executed.size(), "每个任务恰好执行一次");
        System.out.println("[PASS] 旧数据在场时跨键优先级调度保持原样：高优键最先，旧任务按序随后");
    }
}
