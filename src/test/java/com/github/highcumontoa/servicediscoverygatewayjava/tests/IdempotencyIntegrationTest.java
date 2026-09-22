package com.github.highcumontoa.servicediscoverygatewayjava.tests;

import com.github.highcumontoa.servicediscoverygatewayjava.downstream.Behavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.InMemoryDownstreamTransport;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.ScriptedBehavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.SimulatedInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.gateway.ForwardingEngine;
import com.github.highcumontoa.servicediscoverygatewayjava.idempotency.IdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer.RoundRobinLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DispatchRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DispatchResult;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.registry.ServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.retry.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 幂等去重：相同 idempotencyKey 的并发请求共享同一次下游执行。
 */
class IdempotencyIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyIntegrationTest.class);

    @Test
    void concurrentSameKeyExecutesDownstreamOnce() throws Exception {
        ServiceRegistry registry = new ServiceRegistry(3, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();

        // 3 个健康实例，但同一 key 的并发请求只应有一次真正调用。
        // 加入处理延迟以稳定构造“在途窗口”，让其它并发请求必然命中去重。
        for (String id : List.of("i1", "i2", "i3")) {
            ServiceInstance meta = new ServiceInstance(id, "svc", "h", 1);
            registry.register(meta);
            transport.register(new SimulatedInstance(meta, ScriptedBehavior.always(Behavior.NORMAL), 100L));
        }

        ForwardingEngine engine = new ForwardingEngine(registry, transport,
                new RoundRobinLoadBalancer(), new RetryPolicy(3, 0),
                new IdempotencyDeduplicator(), 200);

        int concurrency = 20;
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger duplicated = new AtomicInteger();
        // 所有线程到齐后再统一放行，保证它们看到同一个“在途”执行
        CountDownLatch allReady = new CountDownLatch(concurrency);
        CountDownLatch release = new CountDownLatch(1);
        List<Future<DispatchResult>> futures = new java.util.ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            futures.add(pool.submit(() -> {
                allReady.countDown();
                start.await();
                release.await();
                DispatchResult result = engine.forward(
                        new DispatchRequest("svc", "p", "key-100", true));
                if (result.duplicated()) {
                    duplicated.incrementAndGet();
                }
                return result;
            }));
        }
        allReady.await(5, TimeUnit.SECONDS);
        start.countDown();
        release.countDown();

        DispatchResult winner = null;
        for (Future<DispatchResult> future : futures) {
            DispatchResult result = future.get(5, TimeUnit.SECONDS);
            if (!result.duplicated()) {
                if (winner != null) {
                    fail("只能有一个请求真正执行");
                }
                winner = result;
            }
        }
        assertNotNull(winner);
        int totalInvokes = 0;
        for (String id : List.of("i1", "i2", "i3")) {
            totalInvokes += transport.instance(id).invokeCount();
        }
        // 所有复用结果必须与真正执行的结果一致
        for (Future<DispatchResult> future : futures) {
            DispatchResult result = future.get();
            assertEquals(winner.instanceId(), result.instanceId(), "并发请求应复用同一实例结果");
            assertEquals(winner.body(), result.body());
        }
        pool.shutdown();

        log.info("[T] 输入：{} 个并发请求同 key=key-100；选择结果：总下游调用={} 命中去重={}；依据：在途 future 共享",
                concurrency, totalInvokes, duplicated.get());
        assertEquals(1, totalInvokes, "同一幂等标识并发请求不得重复执行下游");
        assertEquals(concurrency - 1, duplicated.get(), "除首个请求外都应标记为复用");
    }

    @Test
    void sequentialSameKeyAfterCompletionExecutesAgain() {
        // 执行完成后占位释放：同 key 的后续请求应作为新请求执行
        ServiceRegistry registry = new ServiceRegistry(3, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();
        ServiceInstance meta = new ServiceInstance("i1", "svc", "h", 1);
        registry.register(meta);
        transport.register(new SimulatedInstance(meta, ScriptedBehavior.always(Behavior.NORMAL), 0L));

        ForwardingEngine engine = new ForwardingEngine(registry, transport,
                new RoundRobinLoadBalancer(), new RetryPolicy(1, 0),
                new IdempotencyDeduplicator(), 200);

        DispatchRequest req = new DispatchRequest("svc", "p", "key-seq", true);
        engine.forward(req);
        DispatchResult second = engine.forward(req);
        log.info("[T] 串行两次同 key，第二次 duplicated={}，下游调用={}；依据=完成后释放占位",
                second.duplicated(), transport.instance("i1").invokeCount());
        assertFalse(second.duplicated());
        assertEquals(2, transport.instance("i1").invokeCount());
    }
}
