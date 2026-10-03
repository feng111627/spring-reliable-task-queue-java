package com.github.feng111627.springreliabletaskqueuejava.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景（存储层，确定性）：优先级调度与同键保序的交叉。
 * 期望：积压时高优先级先被领取；但同一业务键内部严格先来后到，
 * 后提交的高优先级任务不能越过同键更早的任务；不传优先级时维持创建时间 FIFO。
 * 端到端的真实轮询验证见 {@link PrioritySchedulingEndToEndTest}。
 */
class PrioritySchedulingTest {

    @TempDir
    Path dir;

    private QueueProperties props;
    private FileTaskStore store;

    @BeforeEach
    void setUp() {
        props = TestQueues.fastProps(dir.toString());
        store = TestQueues.newStore(props);
    }

    @Test
    void higherPriorityClaimedFirstAcrossKeys() {
        System.out.println("[CASE] 跨键优先级：不同业务键积压时，高优先级先被领取");
        Instant base = Instant.now();
        // 按低->高的顺序提交，制造积压；彼此业务键不同，互不阻塞
        submitAt("key-low", "p", 0, base);
        submitAt("key-mid", "p", 5, base);
        submitAt("key-high", "p", 10, base);

        // 显式传一个晚于所有到期点的时钟，避免与真实 now 的微秒级抖动耦合
        List<TaskRecord> claimed = store.claimDue(base.plusSeconds(1), 10);
        assertEquals(List.of("key-high", "key-mid", "key-low"),
                claimed.stream().map(TaskRecord::getBusinessKey).toList(),
                "领取次序必须是优先级降序");
        System.out.println("[PASS] 领取次序=" + claimed.stream().map(TaskRecord::getBusinessKey).toList()
                + "，高优先级在积压中先被消费");
    }

    @Test
    void defaultPriorityKeepsFifo() {
        System.out.println("[CASE] 不传优先级：全部 priority=0 时仍按创建时间 FIFO");
        Instant base = Instant.now();
        submitAt("a", "p", 0, base);
        submitAt("b", "p", 0, base.plusMillis(1));
        submitAt("c", "p", 0, base.plusMillis(2));

        List<TaskRecord> claimed = store.claimDue(base.plusSeconds(1), 10);
        assertEquals(List.of("a", "b", "c"),
                claimed.stream().map(TaskRecord::getBusinessKey).toList(),
                "缺省优先级必须与旧行为一致：创建时间 FIFO");
        System.out.println("[PASS] 缺省优先级行为不变，领取次序="
                + claimed.stream().map(TaskRecord::getBusinessKey).toList());
    }

    @Test
    void sameKeyHigherPriorityCannotJumpAhead() {
        System.out.println("[CASE] 同键保序交叉：后提交的高优先级不能越过同键更早的普通任务");
        Instant base = Instant.now();
        submitAt("order-1", "first", 0, base);          // 先提交，普通优先级
        submitAt("order-1", "second-high", 10, base.plusMillis(1)); // 后提交，高优先级

        List<TaskRecord> first = store.claimDue(base.plusSeconds(1), 10);
        assertEquals(1, first.size(), "同键只有队首（最早的一个）任务有资格被领取");
        assertEquals("first", first.get(0).getPayload(), "高优先级的后来者不能越级");
        // 队首已在执行中（RUNNING，非终结），第二个任务仍被同键保序挡住
        assertTrue(store.claimDue(base.plusSeconds(1), 10).isEmpty(),
                "同键前一个任务尚未终结时，后一个任务不能被提前挑走");

        // 前一个执行成功（终结）后，第二个任务才允许被领取
        assertTrue(store.markSucceeded(first.get(0).getId(), first.get(0).getVersion()));
        List<TaskRecord> second = store.claimDue(base.plusSeconds(1), 10);
        assertEquals(1, second.size());
        assertEquals("second-high", second.get(0).getPayload());
        System.out.println("[PASS] 同键严格先来后到：first 先执行并终结后，second-high 才被领取，优先级未打乱同键顺序");
    }

    @Test
    void terminalEarlierTaskReleasesNextOnSameKey() {
        System.out.println("[CASE] 同键前序任务为终态（取消/成功/死信）时不阻塞后续任务");
        Instant base = Instant.now();
        TaskRecord first = submitAt("order-2", "first", 0, base);
        submitAt("order-2", "second", 0, base.plusMillis(1));

        // 取消前序任务后，后续任务立即可领取（CANCELLED 是终态，不再占位）
        assertEquals(CancelResult.Outcome.CANCELLED, store.cancel(first.getId(), null).outcome());
        List<TaskRecord> claimed = store.claimDue(base.plusSeconds(1), 10);
        assertEquals(1, claimed.size());
        assertEquals("second", claimed.get(0).getPayload());
        System.out.println("[PASS] 同键前序任务被取消（终态）后，后续任务正常被领取");
    }

    /** 绕过 service 直接构造记录，用于精确控制 createdAt，保证排序用例确定。 */
    private TaskRecord submitAt(String businessKey, String payload, int priority, Instant createdAt) {
        TaskRecord t = new TaskRecord();
        t.setId(java.util.UUID.randomUUID().toString());
        t.setBusinessKey(businessKey);
        t.setPayload(payload);
        t.setStatus(TaskStatus.PENDING);
        t.setAttempts(0);
        t.setMaxAttempts(props.getMaxAttempts());
        t.setNextAttemptAt(createdAt);
        t.setCreatedAt(createdAt);
        t.setUpdatedAt(createdAt);
        t.setVersion(0);
        t.setPriority(priority);
        t.setNotBefore(null);
        assertEquals(SubmitResult.Outcome.ACCEPTED, store.insertIfAbsent(t).outcome());
        return t;
    }
}
