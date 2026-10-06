package com.github.feng111627.springreliabletaskqueuejava.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 旧版本（尚无 priority/notBefore/seq 字段的 v1 数据）与新版本优先级共同作用时的同业务键顺序。
 */
class LegacyDataOrderingTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private final List<String> executed = new CopyOnWriteArrayList<>();
    private TaskDispatcher dispatcher;
    private FileTaskStore store;

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        // 单消费者分带，保证领取顺序可断言
        props.setConsumerThreads(1);
    }

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.stop();
        }
    }

    /** 按 v1（旧版本）字段集写一条任务 JSON：不含 seq/priority/notBefore。 */
    private static Map<String, Object> legacyRecord(String id, String businessKey, String payload,
                                                   TaskStatus status, Instant createdAt,
                                                   Instant nextAttemptAt, int attempts, int maxAttempts) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("businessKey", businessKey);
        m.put("payload", payload);
        m.put("status", status.name());
        m.put("attempts", attempts);
        m.put("maxAttempts", maxAttempts);
        m.put("nextAttemptAt", nextAttemptAt == null ? null : nextAttemptAt.toString());
        m.put("createdAt", createdAt.toString());
        m.put("updatedAt", createdAt.toString());
        m.put("lastError", null);
        m.put("version", 0);
        return m;
    }

    private void writeLegacyFile(List<Map<String, Object>> records) throws Exception {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        Files.createDirectories(dir);
        mapper.writeValue(dir.resolve("tasks.json").toFile(), records);
    }

    private FileTaskStore openStore() {
        return TestQueues.newStore(props);
    }

    private void startDispatcher(FileTaskStore store) {
        this.store = store;
        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> executed.add(task.getBusinessKey() + "|" + task.getPayload()));
        dispatcher.start();
    }

    @Test
    void oldPendingTaskBlocksHigherPriorityNewTaskButCrossKeyPriorityStays() throws Exception {
        System.out.println("[CASE] 旧数据兼容：v1 旧任务 order-1/old(排队中) 仍在时，"
                + "同键新任务 new(高优10) 不得插队；跨键 urgent-x(高优10) 仍先于默认优先级");
        Instant old = Instant.now().minusSeconds(3600);
        List<Map<String, Object>> legacy = new ArrayList<>();
        legacy.add(legacyRecord("old-1", "order-1", "old", TaskStatus.PENDING, old, old, 0, 3));
        writeLegacyFile(legacy);

        FileTaskStore store = openStore();
        TaskQueueService service = new TaskQueueService(store, props);
        // 先把新任务全部提交，再启动调度器，使首轮轮询同时看到旧任务与三个新任务
        service.submit("order-1", "new", 10, null);
        service.submit("normal-x", "normal", 0, null);
        service.submit("urgent-x", "urgent", 10, null);
        startDispatcher(store);

        TestQueues.await("4 条新/旧任务全部成功",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 4,
                java.time.Duration.ofSeconds(20));

        List<String> payloads = executed.stream().map(x -> x.substring(x.indexOf('|') + 1)).toList();
        System.out.println("[INFO] 实际消费顺序=" + executed);
        // order-1 内部 old 必须先于 new；跨键 urgent-x 高优先级最先，normal-x 与 order-1 队头同级按提交时间
        assertEquals(List.of("urgent", "old", "normal", "new"), payloads,
                "旧任务未终结前同键高优新任务不得插队；跨键高优先级调度保持不变");
        long oldSeq = store.findById("old-1").orElseThrow().getSeq();
        long newSeq = store.findByBusinessKey("order-1").orElseThrow().getSeq();
        assertTrue(oldSeq > 0 && oldSeq < newSeq,
                "旧数据加载后应按提交先后补齐同键序号：old.seq=" + oldSeq + " new.seq=" + newSeq);
        System.out.println("[PASS] order-1: old -> new 严格按提交先后；跨键顺序 urgent-x -> old -> normal-x -> new；旧记录补齐 seq=" + oldSeq);
    }

    @Test
    void oldFailedTaskWaitingForRetryAlsoBlocksHigherPriorityNewTask() throws Exception {
        System.out.println("[CASE] 旧数据兼容：v1 旧任务 order-2/old 处于 FAILED 退避重试中，"
                + "同键新任务 new(高优10, 立即可执行) 在旧任务重试成功前不得被挑走");
        Instant now = Instant.now();
        // 旧任务已失败 1 次，下次可重试时间在 500ms 后；maxAttempts=3，仍可再重试
        List<Map<String, Object>> legacy = List.of(
                legacyRecord("old-2", "order-2", "old", TaskStatus.FAILED,
                        now.minusSeconds(60), now.plusMillis(500), 1, 3));
        writeLegacyFile(legacy);

        FileTaskStore store = openStore();
        TaskQueueService service = new TaskQueueService(store, props);
        service.submit("order-2", "new", 10, null);

        dispatcher = new TaskDispatcher(store, props);
        dispatcher.setHandler(task -> executed.add(task.getBusinessKey() + "|" + task.getPayload()));
        dispatcher.start();

        // 旧任务的重试点（500ms）到达前，立即可执行的高优新任务也必须等着
        Thread.sleep(300);
        assertEquals(TaskStatus.FAILED, store.findById("old-2").orElseThrow().getStatus(),
                "退避窗口内旧任务仍为 FAILED");
        assertTrue(executed.stream().noneMatch(x -> x.endsWith("|new")),
                "旧任务退避重试期间，同键高优新任务不得提前执行，实际=" + executed);
        System.out.println("[INFO] 300ms 时（旧任务仍在退避）已执行=" + executed + "，new 尚未执行");

        TestQueues.await("旧任务重试成功、新任务随后成功",
                () -> store.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 2,
                java.time.Duration.ofSeconds(20));
        List<String> payloads = executed.stream().map(x -> x.substring(x.indexOf('|') + 1)).toList();
        System.out.println("[INFO] 实际消费顺序=" + executed);
        assertEquals(List.of("old", "new"), payloads,
                "FAILED 终态前的旧任务仍占队头，重试成功后高优新任务才可执行");
        System.out.println("[PASS] 旧任务 FAILED 退避中高优新任务被挡住，旧任务重试成功后 new 才执行：old -> new");
    }

    @Test
    void orderingSurvivesCrashRecoveryAndSecondRestartWithMigratedSeq() throws Exception {
        System.out.println("[CASE] 重启恢复：v1 旧任务 order-3/old(PENDING) + 同键高优新任务 new，"
                + "崩溃时旧任务正 RUNNING；重启后仍 old -> new，再重启一次顺序依旧、跨键 urgent-z 仍最先");
        Instant old = Instant.now().minusSeconds(7200);
        writeLegacyFile(List.of(legacyRecord("old-3", "order-3", "old", TaskStatus.PENDING, old, old, 0, 3)));

        // ---- 第一个“进程”：加载旧数据，提交新任务后仅手动领取旧任务并模拟在 RUNNING 中崩溃 ----
        FileTaskStore store1 = openStore();
        TaskQueueService service1 = new TaskQueueService(store1, props);
        service1.submit("order-3", "new", 10, null);
        service1.submit("urgent-z", "urgent", 10, null);
        assertEquals(1, store1.findById("old-3").orElseThrow().getSeq(), "旧记录加载后应补齐 seq=1");
        assertEquals(2, store1.findByBusinessKey("order-3").orElseThrow().getSeq(), "同键新任务应为 seq=2");
        // 首轮领取：new 必须被旧任务挡住，只有 urgent-z 与旧任务可领取；手动只领 1 条并让它成为崩溃残留 RUNNING
        List<TaskRecord> firstClaim = store1.claimDue(Instant.now(), 1);
        assertEquals(1, firstClaim.size());
        assertEquals("urgent-z", firstClaim.get(0).getBusinessKey(),
                "跨键高优先级任务应先于默认优先级旧任务被领取");
        // 再领一批：new 仍被旧任务挡住，轮到旧任务
        List<TaskRecord> secondClaim = store1.claimDue(Instant.now(), 10);
        assertEquals(1, secondClaim.size());
        assertEquals("old-3", secondClaim.get(0).getId(), "同键新任务不得越过旧任务，旧任务此时才能被领取");
        assertEquals(TaskStatus.RUNNING, store1.findById("old-3").orElseThrow().getStatus());
        // 不做 stop、不落完成状态，丢弃 store1 —— 模拟进程在旧任务执行中崩溃
        System.out.println("[INFO] 模拟崩溃：旧任务 old-3 处于 RUNNING，new 仍 PENDING 等待，进程退出");

        // ---- 第二个“进程”：从同一存储恢复（RUNNING 被回收为 PENDING）并实际消费 ----
        FileTaskStore store2 = openStore();
        assertEquals(TaskStatus.RUNNING, store2.findById("old-3").orElseThrow().getStatus(),
                "崩溃残留 RUNNING 状态应能从存储读出");
        assertEquals(1, store2.findById("old-3").orElseThrow().getSeq(), "已迁移的 seq 必须随文件保留");
        assertEquals(2, store2.findByBusinessKey("order-3").orElseThrow().getSeq(),
                "同键新任务 seq=2 也必须随文件保留");
        startDispatcher(store2);

        TestQueues.await("恢复后新旧任务全部成功（共 3 条）",
                () -> store2.listAll().stream().filter(t -> t.getStatus() == TaskStatus.SUCCEEDED).count() == 3,
                java.time.Duration.ofSeconds(20));
        List<String> order3AfterCrash = executed.stream()
                .filter(x -> x.startsWith("order-3|"))
                .map(x -> x.substring(x.indexOf('|') + 1)).toList();
        System.out.println("[INFO] 崩溃恢复后消费顺序=" + executed);
        assertEquals(List.of("old", "new"), order3AfterCrash,
                "RUNNING 旧任务被回收重投后仍必须先于同键高优新任务");
        assertEquals(1, executed.stream().filter("urgent-z|urgent"::equals).count(),
                "崩溃前领取的跨键任务恢复后恰好执行一次");
        dispatcher.stop();

        // ---- 第三个“进程”：再重启一次，确认 seq 已持久化、顺序语义不漂移 ----
        executed.clear();
        FileTaskStore store3 = openStore();
        TaskQueueService service3 = new TaskQueueService(store3, props);
        service3.submit("order-3", "new2", 10, null);
        startDispatcher(store3);
        TestQueues.await("二次重启后新任务 new2 成功",
                () -> executed.contains("order-3|new2"),
                java.time.Duration.ofSeconds(20));
        assertEquals(List.of("new2"), executed.stream().map(x -> x.substring(x.indexOf('|') + 1)).toList(),
                "已终结的 old/new 不重跑，新任务独立执行，顺序语义未因重启漂移");
        System.out.println("[PASS] 崩溃恢复与二次重启后均保持 old -> new；迁移 seq 已持久化，跨键 urgent-z 首轮最先");
    }

    @org.junit.jupiter.params.ParameterizedTest(name = "{0} 的旧任务不再阻塞同键新任务")
    @org.junit.jupiter.params.provider.EnumSource(value = TaskStatus.class,
            names = {"SUCCEEDED", "DEAD", "CANCELLED"})
    void terminalOldTaskDoesNotBlockNewTask(TaskStatus terminal) throws Exception {
        System.out.println("[CASE] 终态边界：v1 旧任务 order-4/old 已处于终态 " + terminal
                + "，同键高优新任务 new 应可直接执行，不被终态前辈阻塞");
        Instant old = Instant.now().minusSeconds(1800);
        // 三种终态都用“无 seq 字段的旧格式记录”构造（CANCELLED 在真实 v1 数据中不会出现，
        // 这里仅作为防御性边界）：加载时同样按提交先后补齐序号，但终态不占队头。
        List<Map<String, Object>> legacy = new ArrayList<>();
        legacy.add(legacyRecord("old-4", "order-4", "old", terminal, old, old,
                terminal == TaskStatus.DEAD ? 3 : 0, 3));
        writeLegacyFile(legacy);

        FileTaskStore store = openStore();
        TaskQueueService service = new TaskQueueService(store, props);
        service.submit("order-4", "new", 10, null);
        startDispatcher(store);

        TestQueues.await("终态旧任务保持原状且新任务成功",
                () -> store.findById("old-4").orElseThrow().getStatus() == terminal
                        && store.findByBusinessKey("order-4").orElseThrow().getStatus() == TaskStatus.SUCCEEDED,
                java.time.Duration.ofSeconds(20));
        assertEquals(List.of("order-4|new"), executed,
                "终态(SUCCEEDED/DEAD/CANCELLED) 旧任务不得再执行，也不得阻塞同键后继，实际=" + executed);
        long oldSeq = store.findById("old-4").orElseThrow().getSeq();
        long newSeq = store.findByBusinessKey("order-4").orElseThrow().getSeq();
        assertEquals(1, oldSeq);
        assertEquals(2, newSeq);
        System.out.println("[PASS] 终态 " + terminal + " 旧任务不再执行也不阻塞：new 直接执行（old.seq=1 < new.seq=2，终态不挡道）");
    }
}
