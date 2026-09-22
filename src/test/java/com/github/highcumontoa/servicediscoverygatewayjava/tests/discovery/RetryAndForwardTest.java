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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 容错转发测试：
 * - TIMEOUT / CONNECTION_REFUSED / DOWNSTREAM_ERROR 三类失败可区分；
 * - 可重试请求受 maxAttempts 限制、重试避开故障实例、最终成功；
 * - 不可重试请求只执行一次，绝不静默重试；
 * - 全部实例熔断/摘除时快速失败且不调用下游；
 * - 5xx 不自动重试，避免重复执行业务。
 */
class RetryAndForwardTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndForwardTest.class);
    private static final String SVC = "svc";

    private record Harness(GatewayForwarder forwarder,
                           SimulatedDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
    }

    private Harness harness() {
        var clock = new AtomicLong(0);
        var downstream = new SimulatedDownstream(0);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registry = new InMemoryServiceRegistry(health, downstream, clock::get);
        var lb = new RoundRobinLoadBalancer();
        var retry = new RetryPolicy(3, 0);
        var circuit = new CircuitBreakerConfig(10, 10_000, 1); // 大多数用例中先不触发熔断
        var forwarder = new GatewayForwarder(registry, lb, downstream, retry, circuit,
                clock::get, 50, millis -> { });
        return new Harness(forwarder, downstream, registry, clock);
    }

    private void register(Harness h, int n) {
        for (int i = 1; i <= n; i++) {
            h.registry.register(SVC, "i" + i);
        }
    }

    @Test
    void retriableRequestRetriesOnDifferentInstanceAndSucceeds() {
        var h = harness();
        register(h, 3);
        h.downstream.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        var request = new ForwardRequest(SVC, null, "p", true);

        ForwardOutcome outcome = h.forwarder.forward(request);
        log.info("input: i1 REFUSE, retryable=true; result: success={}, attempts={}; "
                        + "basis=CONNECTION_REFUSED is transport-retriable, retry avoids i1",
                outcome.success(), outcome.attempts());
        assertTrue(outcome.success());
        assertEquals(2, outcome.attempts().size(), "首轮失败 + 换实例成功 = 2 次尝试");
        assertEquals("i1", outcome.attempts().get(0).instanceId());
        String retryTarget = outcome.attempts().get(1).instanceId();
        assertTrue(!retryTarget.equals("i1"),
                "重试必须避开已失败的 i1，实际选择=" + retryTarget);
        assertEquals(1, h.downstream.invokeCount("i1"));
        assertEquals(1, h.downstream.invokeCount(retryTarget));
    }

    @Test
    void retryBudgetIsCappedAtMaxAttempts() {
        var h = harness();
        register(h, 3);
        for (int i = 1; i <= 3; i++) {
            h.downstream.setMode("i" + i, SimulatedDownstream.Mode.TIMEOUT);
        }
        var outcome = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        log.info("input: all TIMEOUT, retryable=true, maxAttempts=3; result: success={}, type={}, attempts={}; "
                + "basis=retry budget capped, each attempt on a new instance",
                outcome.success(), outcome.failureType(), ids(outcome));
        assertFalse(outcome.success());
        assertEquals(3, outcome.attempts().size(), "最多 3 次尝试，不得无限重试放大压力");
        assertEquals(3, outcome.attempts().stream().map(ForwardOutcome.AttemptRecord::instanceId).distinct().count());
        // 终态归并为 RETRY_EXHAUSTED，但明细保留可区分的 TIMEOUT
        assertEquals(FailureType.RETRY_EXHAUSTED, outcome.failureType());
        assertTrue(outcome.attempts().stream().allMatch(a -> a.result().equals("TIMEOUT")));
    }

    @Test
    void nonRetriableRequestExecutesExactlyOnce() {
        var h = harness();
        register(h, 3);
        h.downstream.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        var outcome = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        log.info("input: i1 REFUSE, retryable=false; result: success={}, type={}, attempts={}, reason={}; "
                        + "basis=non-retriable never silently retried",
                outcome.success(), outcome.failureType(), outcome.attempts().size(), outcome.reason());
        assertFalse(outcome.success());
        assertEquals(1, outcome.attempts().size(), "不可重试请求必须只执行一次");
        assertEquals(FailureType.CONNECTION_REFUSED, outcome.failureType(), "失败原因须原样可区分");
        assertEquals(1, h.downstream.invokeCount("i1"));
        assertEquals(0, h.downstream.invokeCount("i2"));
    }

    @Test
    void downstream5xxIsDistinguishedAndNotAutoRetried() {
        var h = harness();
        register(h, 3);
        h.downstream.setMode("i1", SimulatedDownstream.Mode.ERROR_500);
        var outcome = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        log.info("input: i1 ERROR_500, retryable=true; result: success={}, type={}, attempts={}; "
                        + "basis=5xx executed business logic, not auto-retried to prevent duplicate",
                outcome.success(), outcome.failureType(), outcome.attempts().size());
        assertFalse(outcome.success());
        assertEquals(FailureType.DOWNSTREAM_ERROR, outcome.failureType(), "5xx 必须映射为可区分的 DOWNSTREAM_ERROR");
        assertEquals(1, outcome.attempts().size());
        assertEquals(1, h.downstream.payloadExecutionCount("i1", "p"));
    }

    @Test
    void noHealthyInstanceFailsFastWithoutCallingDownstream() {
        var h = harness();
        // 不注册任何实例
        var outcome = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        log.info("input: no registered instance; result: type={}; basis=NO_HEALTHY_INSTANCE fail-fast",
                outcome.failureType());
        assertFalse(outcome.success());
        assertEquals(FailureType.NO_HEALTHY_INSTANCE, outcome.failureType());
        assertTrue(outcome.attempts().isEmpty());
    }

    @Test
    void allCircuitsOpenFailsFastAndDoesNotHitDownstream() {
        var h = harness();
        register(h, 2);
        h.forwarder.forceOpen(SVC, "i1");
        h.forwarder.forceOpen(SVC, "i2");
        int before1 = h.downstream.invokeCount("i1");
        var outcome = h.forwarder.forward(new ForwardRequest(SVC, null, "p", true));
        log.info("input: both instances circuit OPEN (cooldown 10s); result: type={}, attempts={}; "
                        + "basis=CIRCUIT_OPEN fail-fast, zero downstream calls",
                outcome.failureType(), outcome.attempts().size());
        assertFalse(outcome.success());
        assertEquals(FailureType.CIRCUIT_OPEN, outcome.failureType());
        assertTrue(outcome.attempts().isEmpty());
        assertEquals(before1, h.downstream.invokeCount("i1"), "熔断期间不得向下stream放大流量");
    }

    private static java.util.List<String> ids(ForwardOutcome outcome) {
        return outcome.attempts().stream().map(ForwardOutcome.AttemptRecord::instanceId).toList();
    }
}
