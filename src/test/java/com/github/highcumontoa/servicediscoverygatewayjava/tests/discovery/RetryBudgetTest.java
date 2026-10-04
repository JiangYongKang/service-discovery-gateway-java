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
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.RetryBudget;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.RetryBudgetConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.SlidingWindowRetryBudget;
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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 按服务的重试预算测试：
 * - 窗口内自动重试总量耗尽后，首发照常、自动重试被拦（保留真实失败类型与依据）；
 * - 预算绝不拦截首发请求；
 * - 窗口滑过后自动重试自行恢复；
 * - 占比上限语义与并发消耗不超发。
 */
class RetryBudgetTest {

    private static final Logger log = LoggerFactory.getLogger(RetryBudgetTest.class);
    private static final String SVC = "svc";

    private record Harness(GatewayForwarder forwarder,
                           SimulatedDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
    }

    private Harness harness(RetryBudgetConfig budgetConfig) {
        var clock = new AtomicLong(1000);
        var downstream = new SimulatedDownstream(0);
        var registry = new InMemoryServiceRegistry(
                new HealthConfig(3, 1000, 100, 50), downstream, clock::get);
        var forwarder = new GatewayForwarder(registry, new RoundRobinLoadBalancer(), downstream,
                new RetryPolicy(3, 0),
                new CircuitBreakerConfig(1000, 60_000, 1), // 本类不触发熔断
                clock::get, 50, millis -> { },
                new SlidingWindowRetryBudget(budgetConfig));
        return new Harness(forwarder, downstream, registry, clock);
    }

    private static ForwardRequest req(String payload) {
        return new ForwardRequest(SVC, null, payload, true);
    }

    @Test
    void budgetExhaustsWithinWindowAndRecoversAfterWindow() {
        var h = harness(new RetryBudgetConfig(1000, 2, -1)); // 窗口内最多 2 次自动重试
        for (int i = 1; i <= 3; i++) {
            h.registry.register(SVC, "i" + i);
            h.downstream.setMode("i" + i, SimulatedDownstream.Mode.TIMEOUT);
        }

        ForwardOutcome first = h.forwarder.forward(req("r1"));
        log.info("input: 3 instances TIMEOUT, budget=2 retries/1000ms; result: attempts={}, type={}; "
                + "basis=2 retries consumed, budget now exhausted", first.attempts().size(), first.failureType());
        assertFalse(first.success());
        assertEquals(3, first.attempts().size(), "首个请求用满 2 次重试预算");
        assertEquals(FailureType.RETRY_EXHAUSTED, first.failureType());

        ForwardOutcome second = h.forwarder.forward(req("r2"));
        log.info("input: same window, budget exhausted; result: attempts={}, type={}, reason={}; "
                        + "basis=first attempt executed, auto-retry suppressed by budget",
                second.attempts().size(), second.failureType(), second.reason());
        assertFalse(second.success());
        assertEquals(1, second.attempts().size(), "预算耗尽后不再自动换实例重试");
        assertEquals(FailureType.TIMEOUT, second.failureType(), "必须保留真实失败类型而非预算错误");
        assertTrue(second.reason().contains("retry budget"), "判定依据必须说明是预算拦截");

        h.clock.addAndGet(1000); // 窗口滑过
        ForwardOutcome third = h.forwarder.forward(req("r3"));
        log.info("input: window elapsed; result: attempts={}, type={}; "
                + "basis=budget window slid, auto-retry recovered", third.attempts().size(), third.failureType());
        assertFalse(third.success());
        assertEquals(3, third.attempts().size(), "窗口过去后自动重试必须恢复");
        assertEquals(FailureType.RETRY_EXHAUSTED, third.failureType());
    }

    @Test
    void budgetNeverBlocksFirstAttempt() {
        var h = harness(new RetryBudgetConfig(1000, 1, -1)); // 窗口内只够 1 次重试
        for (int i = 1; i <= 3; i++) {
            h.registry.register(SVC, "i" + i);
            h.downstream.setMode("i" + i, SimulatedDownstream.Mode.TIMEOUT);
        }

        ForwardOutcome storm = h.forwarder.forward(req("storm"));
        assertEquals(2, storm.attempts().size(), "1 次首发 + 1 次预算内重试，之后预算耗尽");
        log.info("input: budget=1 retry/1000ms, all TIMEOUT; result: attempts={}; "
                + "basis=budget exhausted after 1 retry", storm.attempts().size());

        // 预算耗尽期间：下游恢复，新请求的首发必须照常成功，不被预算误判
        for (int i = 1; i <= 3; i++) {
            h.downstream.setMode("i" + i, SimulatedDownstream.Mode.NORMAL);
        }
        ForwardOutcome recovered = h.forwarder.forward(req("healthy-now"));
        log.info("input: budget still exhausted but downstream healthy; result: success={}, attempts={}; "
                + "basis=budget only gates retries, never first attempts",
                recovered.success(), recovered.attempts().size());
        assertTrue(recovered.success(), "预算只能拦重试，不能把本来能成功的首发判成失败");
        assertEquals(1, recovered.attempts().size());
    }

    @Test
    void ratioCapLimitsRetryShare() {
        RetryBudget budget = new SlidingWindowRetryBudget(new RetryBudgetConfig(1000, -1, 0.5));
        long t0 = 10_000;

        budget.recordFirstAttempt(SVC, t0);
        RetryBudget.Decision deniedAtOneFirst = budget.tryConsumeRetry(SVC, t0);
        log.info("input: firsts=1, ratioCap=0.5; result: allowed={}; basis=1 retry > 50% of 1 first",
                deniedAtOneFirst.allowed());
        assertFalse(deniedAtOneFirst.allowed(), "1 次首发不允许任何重试（占比 1/1 > 0.5）");

        budget.recordFirstAttempt(SVC, t0);
        RetryBudget.Decision allowedAtTwoFirsts = budget.tryConsumeRetry(SVC, t0);
        log.info("input: firsts=2, retries=0, ratioCap=0.5; result: allowed={}; basis=1 <= 50% of 2",
                allowedAtTwoFirsts.allowed());
        assertTrue(allowedAtTwoFirsts.allowed());

        RetryBudget.Decision deniedAtCap = budget.tryConsumeRetry(SVC, t0);
        log.info("input: firsts=2, retries=1, ratioCap=0.5; result: allowed={}; basis=2 > 50% of 2",
                deniedAtCap.allowed());
        assertFalse(deniedAtCap.allowed(), "重试占比到顶后必须拦截");

        // 窗口滑过：旧窗口的首发/重试计数都清零；新窗口内先有 2 次首发（真实转发流程
        // 必然先记录首发再判定重试），重试额度随之恢复
        budget.recordFirstAttempt(SVC, t0 + 1000);
        budget.recordFirstAttempt(SVC, t0 + 1000);
        RetryBudget.Decision afterWindow = budget.tryConsumeRetry(SVC, t0 + 1000);
        log.info("input: window slid past 1000ms, firsts=2 in new window; result: allowed={}; "
                + "basis=window counts reset, ratio budget recovered", afterWindow.allowed());
        assertTrue(afterWindow.allowed(), "窗口滑过后占比预算自动恢复");
    }

    @Test
    void concurrentConsumptionNeverExceedsCap() throws Exception {
        RetryBudget budget = new SlidingWindowRetryBudget(new RetryBudgetConfig(60_000, 50, -1));
        long t0 = 10_000;
        int threads = 16;
        int perThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger allowed = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                gate.await();
                for (int i = 0; i < perThread; i++) {
                    if (budget.tryConsumeRetry(SVC, t0).allowed()) {
                        allowed.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        gate.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
        log.info("input: {} threads x {} tryConsumeRetry, cap=50; result: allowed={}; "
                + "basis=atomic check-and-consume under concurrency", threads, perThread, allowed.get());
        assertEquals(50, allowed.get(), "并发消耗绝不得超过预算上限");
    }
}
