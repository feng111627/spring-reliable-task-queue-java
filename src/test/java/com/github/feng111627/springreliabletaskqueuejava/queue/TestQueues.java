package com.github.feng111627.springreliabletaskqueuejava.queue;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * 测试支撑：快速轮询的队列组件 + 条件等待。
 */
final class TestQueues {

    private TestQueues() {
    }

    static QueueProperties fastProps(String storageDir) {
        QueueProperties p = new QueueProperties();
        p.setStorageDir(storageDir);
        p.setConsumerThreads(4);
        p.setMaxAttempts(3);
        p.setBackoffBaseMs(50);
        p.setBackoffMaxMs(200);
        p.setPollIntervalMs(20);
        p.setPollBatchSize(100);
        return p;
    }

    static FileTaskStore newStore(QueueProperties props) {
        FileTaskStore store = new FileTaskStore(props);
        store.load();
        return store;
    }

    static void await(String what, BooleanSupplier condition) {
        await(what, condition, Duration.ofSeconds(15));
    }

    static void await(String what, BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("等待被中断: " + what);
            }
        }
        fail("等待超时: " + what);
    }
}
