package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway.GatewayForwarder;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.InMemoryServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.RoundRobinLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardOutcome;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.HealthConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端组合链路：
 * - 转发流量驱动单实例熔断 -> OPEN 期零下游调用 -> 冷却后半开探测成功恢复；
 * - 半开探测失败时保持熔断；
 * - 同幂等键并发请求在真实下游只执行一次；
 * - 健康探测摘除实例后，流量平滑收敛到剩余实例。
 */
class GatewayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(GatewayIntegrationTest.class);
    private static final String SVC = "svc";

    private record Harness(GatewayForwarder forwarder,
                           SimulatedDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
    }

    private Harness harness(int circuitThreshold, long openMillis) {
        var clock = new AtomicLong(1000);
        var downstream = new SimulatedDownstream(0);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registry = new InMemoryServiceRegistry(health, downstream, clock::get);
        var forwarder = new GatewayForwarder(registry, new RoundRobinLoadBalancer(), downstream,
                new RetryPolicy(1, 0), // 本类聚焦熔断/去重，先关闭自动重试
                new CircuitBreakerConfig(circuitThreshold, openMillis, 1),
                clock::get, 50, millis -> { });
        return new Harness(forwarder, downstream, registry, clock);
    }

    private ForwardRequest req(boolean retryable, String key, String payload) {
        return new ForwardRequest(SVC, key, payload, retryable);
    }

    @Test
    void circuitOpensTrafficStopsThenHalfOpenTrialRecovers() {
        var h = harness(3, 2000);
        h.registry.register(SVC, "i1");
        h.downstream.setMode("i1", SimulatedDownstream.Mode.REFUSE);

        for (int i = 1; i <= 3; i++) {
            ForwardOutcome o = h.forwarder.forward(req(false, null, "p"));
            assertFalse(o.success());
        }
        assertEquals(CircuitState.OPEN.name(), h.forwarder.circuitView(SVC, "i1").state());
        log.info("input: 3 refused forwards; result: circuit i1=OPEN; basis=threshold 3 reached");

        int callsBefore = h.downstream.invokeCount("i1");
        ForwardOutcome rejected = h.forwarder.forward(req(false, null, "p"));
        int callsAfter = h.downstream.invokeCount("i1");
        log.info("input: forward while OPEN; result: failureType={}, downstreamCallsDelta={}; "
                + "basis=OPEN fail-fast, zero pressure", rejected.failureType(), callsAfter - callsBefore);
        assertEquals(FailureType.CIRCUIT_OPEN, rejected.failureType());
        assertEquals(callsBefore, callsAfter, "熔断打开期间不得再调用下游");

        h.clock.addAndGet(2000);
        h.downstream.setMode("i1", SimulatedDownstream.Mode.NORMAL);

        ForwardOutcome trial = h.forwarder.forward(req(false, null, "p"));
        log.info("input: cooldown elapsed + instance recovered; result: trial.success={}, circuit={}; "
                        + "basis=single half-open trial success closes circuit",
                trial.success(), h.forwarder.circuitView(SVC, "i1").state());
        assertTrue(trial.success());
        assertEquals(CircuitState.CLOSED.name(), h.forwarder.circuitView(SVC, "i1").state());
    }

    @Test
    void failedHalfOpenTrialKeepsCircuitOpen() {
        var h = harness(1, 2000);
        h.registry.register(SVC, "i1");
        h.downstream.setMode("i1", SimulatedDownstream.Mode.TIMEOUT);

        assertFalse(h.forwarder.forward(req(false, null, "p")).success());
        assertEquals(CircuitState.OPEN.name(), h.forwarder.circuitView(SVC, "i1").state());

        h.clock.addAndGet(2000); // 仍未恢复 -> 半开探测失败
        ForwardOutcome trial = h.forwarder.forward(req(false, null, "p"));
        log.info("input: half-open trial against still-broken instance; result: success={}, circuit={}; "
                        + "basis=failed trial returns to OPEN",
                trial.success(), h.forwarder.circuitView(SVC, "i1").state());
        assertFalse(trial.success());
        assertEquals(CircuitState.OPEN.name(), h.forwarder.circuitView(SVC, "i1").state(),
                "半开探测失败必须保持熔断");

        boolean stillRejected = h.forwarder.forward(req(false, null, "p")).failureType()
                == FailureType.CIRCUIT_OPEN;
        log.info("input: forward immediately after failed trial; result: rejectedByOpen={}; "
                + "basis=new cooldown restarts", stillRejected);
        assertTrue(stillRejected);
    }

    @Test
    void concurrentIdempotentRequestsExecuteDownstreamOnceThroughGateway() throws Exception {
        var h = harness(10, 10_000);
        h.registry.register(SVC, "i1");
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<ForwardOutcome>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                gate.await();
                return h.forwarder.forward(req(true, "order-7788", "create-order"));
            }));
        }
        gate.countDown();
        int successes = 0;
        for (Future<ForwardOutcome> f : futures) {
            ForwardOutcome o = f.get();
            if (o.success()) {
                successes++;
            }
        }
        pool.shutdown();
        int executions = h.downstream.payloadExecutionCount("i1", "create-order");
        log.info("input: {} concurrent forwards with idempotencyKey=order-7788; result: successOutcomes={}, "
                        + "realDownstreamExecutions={}; basis=single leader execution chain",
                threads, successes, executions);
        assertEquals(threads, successes, "所有并发请求都应拿到同一份成功结果");
        assertEquals(1, executions, "同一幂等键的并发请求不得造成重复执行");
    }

    @Test
    void trafficConvergesAfterHealthProbeEviction() {
        var h = harness(100, 10_000);
        h.registry.register(SVC, "i1");
        h.registry.register(SVC, "i2");
        h.downstream.setMode("i1", SimulatedDownstream.Mode.REFUSE);

        // i1 连续 3 次探测失败 -> 摘除
        for (int round = 0; round < 3; round++) {
            h.registry.probeOnce(SVC);
        }
        log.info("input: i1 failed 3 probes; result: snapshot={}; basis=evicted by health system",
                h.registry.snapshot(SVC));

        for (int i = 0; i < 6; i++) {
            ForwardOutcome o = h.forwarder.forward(req(false, null, "p"));
            assertTrue(o.success(), "摘除 i1 后剩余健康实例必须继续承载流量");
            assertEquals("i2", o.attempts().get(0).instanceId(),
                    "已摘除实例不得再被选中，流量平滑收敛到 i2");
        }
        log.info("input: 6 forwards after eviction; result: all served by i2; downstream i1 calls during forwards={}",
                h.downstream.invokeCount("i1") - 3); // 减去 3 次健康探测
    }
}
