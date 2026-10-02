package com.github.feng111627.springreliabletaskqueuejava.tests.taskqueue;

import com.github.feng111627.springreliabletaskqueuejava.taskqueue.FileTaskStore;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.SubmitResult;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.SubmitStatus;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskQueueProperties;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskQueueService;
import com.github.feng111627.springreliabletaskqueuejava.taskqueue.TaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 用例：重复提交幂等去重。
 * 同一业务键重复提交只能真正入队一次，重复提交返回 DUPLICATE 且携带已存在任务。
 */
class DuplicateSubmitTest {

    @TempDir
    Path tempDir;

    private FileTaskStore store;
    private TaskQueueService service;

    @BeforeEach
    void setUp() {
        TaskQueueProperties properties = new TaskQueueProperties();
        properties.setStoragePath(tempDir.resolve("queue.json").toString());
        store = new FileTaskStore(properties);
        store.recover();
        service = new TaskQueueService(store, properties);
    }

    @Test
    void duplicateSubmitIsDetectedAndNotEnqueuedAgain() {
        System.out.println("[用例] 重复提交：同一业务键提交两次");
        SubmitResult first = service.submit("order-1", "payload-1");
        SubmitResult second = service.submit("order-1", "payload-1");

        System.out.println("[结果] 第一次=" + first.getStatus() + ", 第二次=" + second.getStatus());
        assertEquals(SubmitStatus.ACCEPTED, first.getStatus());
        assertEquals(SubmitStatus.DUPLICATE, second.getStatus());
        // 去重结果要携带已存在的任务，便于调用方追踪
        assertEquals(first.getTask().getId(), second.getTask().getId());
        // 存储中只有一条任务，没有悄悄再跑一遍
        assertEquals(1, service.listAll().size());
        System.out.println("[通过] 重复提交被识别为 DUPLICATE，且只入队一次");
    }

    @Test
    void concurrentDuplicateSubmitsOnlyOneAccepted() throws InterruptedException {
        System.out.println("[用例] 并发重复提交：16 个线程同时提交同一业务键");
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        List<SubmitResult> results = new CopyOnWriteArrayList<>();
        List<Thread> pool = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                results.add(service.submit("order-concurrent", "p"));
            });
            pool.add(t);
            t.start();
        }
        start.countDown();
        for (Thread t : pool) {
            t.join(TimeUnit.SECONDS.toMillis(5));
        }

        long accepted = results.stream().filter(SubmitResult::isAccepted).count();
        long duplicate = results.stream().filter(r -> r.getStatus() == SubmitStatus.DUPLICATE).count();
        System.out.println("[结果] ACCEPTED=" + accepted + ", DUPLICATE=" + duplicate);
        assertEquals(1, accepted);
        assertEquals(threads - 1, duplicate);
        assertEquals(1, service.listAll().size());
        System.out.println("[通过] 并发下仅一个提交真正入队");
    }

    @Test
    void sameKeyCanBeSubmittedAgainAfterSuccess() {
        System.out.println("[用例] 任务成功后，同一业务键可再次提交（新任务）");
        SubmitResult first = service.submit("order-2", "p1");
        // 模拟任务走完生命周期：PENDING -> RUNNING -> SUCCEEDED
        store.claimNextDue(Set.of());
        store.markSucceeded(first.getTask().getId());

        SubmitResult second = service.submit("order-2", "p2");
        System.out.println("[结果] 成功后再次提交=" + second.getStatus());
        assertEquals(SubmitStatus.ACCEPTED, second.getStatus());
        assertEquals(TaskStatus.PENDING, second.getTask().getStatus());
        assertEquals(2, service.listAll().size());
        System.out.println("[通过] 终态任务不阻塞同键新任务");
    }
}
