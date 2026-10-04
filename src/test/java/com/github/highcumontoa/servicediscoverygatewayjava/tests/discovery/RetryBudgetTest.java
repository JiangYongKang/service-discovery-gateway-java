package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway.GatewayForwarder;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.InMemoryServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.RoundRobinLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardOutcome;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.HealthConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryBudgetPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.SlidingWindowRetryBudget;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 按服务的滑动窗口重试预算测试：
 * - 窗口内重试总量耗尽后：首发照常发出，但不再自动换实例重试；
 * - 重试占比超限同样拦截自动重试；
 * - 窗口滑过后自动重试自行恢复；
 * - 预算判定并发安全（多线程同时抢预算不超发）。
 */
class RetryBudgetTest {

    private static final Logger log = LoggerFactory.getLogger(RetryBudgetTest.class);
    private static final String SVC = "svc";

    private record Harness(GatewayForwarder forwarder,
                           SimulatedDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
    }

    private Harness harness(RetryBudgetPolicy budgetPolicy, int maxAttempts) {
        var clock = new AtomicLong(0);
        var downstream = new SimulatedDownstream(0);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registry = new InMemoryServiceRegistry(health, downstream, clock::get);
        var forwarder = new GatewayForwarder(registry, new RoundRobinLoadBalancer(), downstream,
                new RetryPolicy(maxAttempts, 0),
                new CircuitBreakerConfig(100, 10_000, 1),
                clock::get, 50, millis -> { },
                new SlidingWindowRetryBudget(budgetPolicy));
        return new Harness(forwarder, downstream, registry, clock);
    }

    private void registerAllRefusing(Harness h, int n) {
        for (int i = 1; i <= n; i++) {
            h.registry.register(SVC, "i" + i);
            h.downstream.setMode("i" + i, SimulatedDownstream.Mode.REFUSE);
        }
    }

    @Test
    void budgetExhaustedSuppressesRetryButFirstAttemptStillFires() {
        // 窗口 10s 内最多 2 次自动重试；单请求最多 5 次尝试
        var h = harness(new RetryBudgetPolicy(10_000, 2, 1.0), 5);
        registerAllRefusing(h, 5);

        ForwardOutcome first = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        log.info("input: all REFUSE, budget=2 retries/10s, maxAttempts=5; result: attempts={}, type={}; "
                + "basis=first request consumes the whole retry budget",
                first.attempts().size(), first.failureType());
        assertFalse(first.success());
        assertEquals(3, first.attempts().size(), "首发 + 2 次预算内重试，第 3 次重试被预算拦截");
        assertEquals(FailureType.CONNECTION_REFUSED, first.failureType(),
                "预算拦截时保留真实失败类型");
        assertTrue(first.reason().contains("retry budget"), "判定依据必须说明是预算拦截");

        int downstreamCallsBefore = totalInvokeCount(h, 5);
        ForwardOutcome second = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        int downstreamCallsAfter = totalInvokeCount(h, 5);
        log.info("input: budget exhausted, new request arrives; result: attempts={}, type={}, reason={}, "
                        + "downstreamDelta={}; basis=first attempt still fires, auto-retry suppressed",
                second.attempts().size(), second.failureType(), second.reason(),
                downstreamCallsAfter - downstreamCallsBefore);
        assertFalse(second.success());
        assertEquals(1, second.attempts().size(), "预算耗尽后不再自动重试");
        assertEquals(1, downstreamCallsAfter - downstreamCallsBefore,
                "首发请求必须照常发出，不能被预算误判");
        assertEquals(FailureType.CONNECTION_REFUSED, second.failureType(),
                "保留真实失败类型而非预算错误");
        assertTrue(second.reason().contains("retry budget"), "判定依据必须说明是预算拦截");
    }

    @Test
    void budgetRecoversAfterWindowSlides() {
        var h = harness(new RetryBudgetPolicy(1_000, 1, 1.0), 5);
        registerAllRefusing(h, 5);

        ForwardOutcome first = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        assertEquals(2, first.attempts().size(), "首发 + 1 次预算内重试");

        ForwardOutcome blocked = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        assertEquals(1, blocked.attempts().size(), "窗口内预算已耗尽");

        h.clock.addAndGet(1_001); // 窗口滑过
        ForwardOutcome recovered = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        log.info("input: window slid past 1000ms; result: attempts={}; basis=budget window expired, "
                + "auto-retry recovers by itself", recovered.attempts().size());
        assertEquals(2, recovered.attempts().size(), "窗口过后自动重试必须恢复");
    }

    @Test
    void retryRatioCapAlsoSuppressesRetry() {
        // 总量很宽松，但重试/首发占比不得超过 50%
        var h = harness(new RetryBudgetPolicy(10_000, 100, 0.5), 5);
        registerAllRefusing(h, 5);

        ForwardOutcome first = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        log.info("input: ratio cap 0.5, first request; result: attempts={}, reason={}; "
                        + "basis=(0 retries+1)/1 request = 1.0 > 0.5, suppressed immediately",
                first.attempts().size(), first.reason());
        assertEquals(1, first.attempts().size(), "占比 1/1=1.0 超过 0.5，首次即被拦截");

        ForwardOutcome second = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        log.info("input: second request, requests=2 retries=0; result: attempts={}; "
                + "basis=(0+1)/2 = 0.5 <= 0.5, one retry allowed", second.attempts().size());
        assertEquals(2, second.attempts().size(), "首发数摊薄后允许 1 次重试");
    }

    @Test
    void budgetAccountingIsConcurrencySafe() throws Exception {
        // 预算仅 10 次重试；32 线程同时首发失败并争抢重试预算
        var h = harness(new RetryBudgetPolicy(60_000, 10, 1.0), 2);
        registerAllRefusing(h, 5);

        int threads = 32;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        var gate = new java.util.concurrent.CountDownLatch(1);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<ForwardOutcome>>();
        for (int i = 0; i < threads; i++) {
            int idx = i;
            futures.add(pool.submit(() -> {
                gate.await();
                return h.forwarder.forward(new ForwardRequest(SVC, "c-" + idx, "p", true));
            }));
        }
        gate.countDown();
        int retried = 0;
        int firstOnly = 0;
        for (var f : futures) {
            ForwardOutcome o = f.get();
            if (o.attempts().size() == 2) {
                retried++;
            } else if (o.attempts().size() == 1) {
                firstOnly++;
            }
        }
        pool.shutdown();
        log.info("input: {} concurrent requests, retry budget=10; result: retried={}, firstOnly={}; "
                + "basis=budget never oversold under concurrency", threads, retried, firstOnly);
        assertEquals(10, retried, "并发下重试预算绝不超发");
        assertEquals(threads - 10, firstOnly, "其余请求只发首发");
    }

    private static int totalInvokeCount(Harness h, int instances) {
        int total = 0;
        for (int i = 1; i <= instances; i++) {
            total += h.downstream.invokeCount("i" + i);
        }
        return total;
    }
}
