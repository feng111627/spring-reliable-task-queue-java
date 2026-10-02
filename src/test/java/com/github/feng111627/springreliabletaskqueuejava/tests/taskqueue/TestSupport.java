package com.github.feng111627.springreliabletaskqueuejava.tests.taskqueue;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;

/** 测试工具：轮询等待异步条件达成 */
final class TestSupport {

    private TestSupport() {
    }

    static void await(String description, BooleanSupplier condition) {
        await(description, Duration.ofSeconds(10), condition);
    }

    static void await(String description, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("等待被中断: " + description);
            }
        }
        fail("等待超时: " + description);
    }
}
