package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamResponse;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamException;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.InMemoryIdempotencyDeduplicator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 幂等去重：同键并发只执行一次、后到者共享结果、失败不缓存允许重试。 */
class IdempotencyDedupTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyDedupTest.class);

    @Test
    void concurrentSameKeyExecutesExactlyOnce() throws Exception {
        var dedup = new InMemoryIdempotencyDeduplicator<DownstreamResponse>();
        int threads = 16;
        AtomicInteger executions = new AtomicInteger();
        CountDownLatch gate = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<DownstreamResponse>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return dedup.execute("key-123", () -> {
                    executions.incrementAndGet();
                    try {
                        Thread.sleep(30);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return new DownstreamResponse(200, "created-order-X");
                });
            }));
        }
        gate.countDown();
        for (Future<DownstreamResponse> f : futures) {
            assertEquals("created-order-X", f.get().body());
        }
        pool.shutdown();
        log.info("input: {} concurrent requests with same idempotency key; result: realExecutions={}; "
                + "basis=computeIfAbsent elects single leader", threads, executions.get());
        assertEquals(1, executions.get(), "同一幂等键的并发请求必须且只能真实执行一次");
        assertEquals(1, dedup.cachedKeys());
    }

    @Test
    void differentKeysExecuteIndependently() {
        var dedup = new InMemoryIdempotencyDeduplicator<DownstreamResponse>();
        AtomicInteger executions = new AtomicInteger();
        for (String key : List.of("a", "b", "c")) {
            dedup.execute(key, () -> {
                executions.incrementAndGet();
                return new DownstreamResponse(200, key);
            });
        }
        log.info("input: 3 distinct keys; result: realExecutions={}; basis=different keys do not dedup",
                executions.get());
        assertEquals(3, executions.get());
    }

    @Test
    void failedEntryIsRemovedAndRetriableLater() {
        var dedup = new InMemoryIdempotencyDeduplicator<DownstreamResponse>();
        AtomicInteger executions = new AtomicInteger();

        assertThrows(DownstreamException.class, () -> dedup.execute("fail-key", () -> {
            executions.incrementAndGet();
            throw new DownstreamException(FailureType.TIMEOUT, "boom");
        }));
        assertThrows(DownstreamException.class, () -> dedup.execute("fail-key", () -> {
            executions.incrementAndGet();
            throw new DownstreamException(FailureType.TIMEOUT, "boom again");
        }));
        log.info("input: same key failed twice sequentially; result: realExecutions={}, cachedKeys={}; "
                + "basis=failed keys are evicted, no poison cache", executions.get(), dedup.cachedKeys());
        assertEquals(2, executions.get(), "失败条目不缓存，后续请求必须能再次真实执行");
        assertEquals(0, dedup.cachedKeys());
    }
}
