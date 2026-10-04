package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.DownstreamHandler;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway.GatewayForwarder;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.InMemoryServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.WeightedRoundRobinLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamResponse;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 熔断恢复探测链路测试：
 * 先熔断 -> 冷却到期 -> 经过权重与容量筛选后仍真实探测 -> 探测成功恢复接流量；
 * 探测窗口内并发请求不得一拥而上；探测失败保持熔断并重新计时；
 * 只读运行视图可观测全过程且本身不改变任何状态。
 */
class CircuitProbeRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitProbeRecoveryTest.class);
    private static final String SVC = "svc";

    /** 可按实例阻塞的下游，用于把探测请求卡在 HALF_OPEN 窗口内观察并发行为。 */
    private static class BlockingDownstream implements DownstreamHandler {
        private final SimulatedDownstream delegate = new SimulatedDownstream(0);
        private final Map<String, CountDownLatch> entered = new ConcurrentHashMap<>();
        private final Map<String, CountDownLatch> release = new ConcurrentHashMap<>();
        private final Map<String, Integer> entryCounts = new ConcurrentHashMap<>();

        void blockOn(String instanceId) {
            entered.put(instanceId, new CountDownLatch(1));
            release.put(instanceId, new CountDownLatch(1));
        }

        void unblock(String instanceId) {
            release.get(instanceId).countDown();
        }

        CountDownLatch entered(String instanceId) {
            return entered.get(instanceId);
        }

        /** 进入下游包装层的次数（阻塞前即计数，用于断言探测是否真实到达）。 */
        int invokeCount(String instanceId) {
            return entryCounts.getOrDefault(instanceId, 0);
        }

        void setMode(String instanceId, SimulatedDownstream.Mode mode) {
            delegate.setMode(instanceId, mode);
        }

        @Override
        public DownstreamResponse invoke(String instanceId, String payload, long timeoutMillis) {
            entryCounts.merge(instanceId, 1, Integer::sum);
            CountDownLatch enteredLatch = entered.get(instanceId);
            if (enteredLatch != null) {
                enteredLatch.countDown();
            }
            CountDownLatch releaseLatch = release.get(instanceId);
            if (releaseLatch != null) {
                try {
                    releaseLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.invoke(instanceId, payload, timeoutMillis);
        }
    }

    private record Harness(GatewayForwarder forwarder,
                           BlockingDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
    }

    private Harness harness(int circuitThreshold, long openMillis) {
        var clock = new AtomicLong(1000);
        var downstream = new BlockingDownstream();
        var registry = new InMemoryServiceRegistry(
                new HealthConfig(3, 1000, 100, 50), downstream, clock::get);
        var forwarder = new GatewayForwarder(registry, new WeightedRoundRobinLoadBalancer(), downstream,
                new RetryPolicy(1, 0), // 每请求一次尝试，聚焦熔断探测链路
                new CircuitBreakerConfig(circuitThreshold, openMillis, 1),
                clock::get, 50, millis -> { });
        return new Harness(forwarder, downstream, registry, clock);
    }

    private static ForwardRequest req(String payload) {
        return new ForwardRequest(SVC, null, payload, false);
    }

    private GatewayForwarder.InstanceView viewOf(Harness h, String instanceId) {
        return h.forwarder.runtimeView(SVC).instances().stream()
                .filter(v -> v.instanceId().equals(instanceId)).findFirst().orElseThrow();
    }

    @Test
    void probeIsPinnedThroughWeightAndCapacityScreeningThenRecovers() throws Exception {
        var h = harness(3, 2000);
        // 先只注册 i1（权重 1、容量上限 1），用 3 次真实失败把它熔断
        h.registry.register(SVC, "i1", 1, 1);
        h.downstream.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        for (int i = 0; i < 3; i++) {
            assertFalse(h.forwarder.forward(req("f" + i)).success());
        }
        assertEquals(CircuitState.OPEN.name(), viewOf(h, "i1").circuitState());
        log.info("input: 3 refused forwards on i1 (w=1, maxInFlight=1); result: circuit=OPEN, "
                + "failureDistribution={}; basis=threshold 3 reached",
                viewOf(h, "i1").recentFailureDistribution());
        assertEquals(3, viewOf(h, "i1").recentFailureDistribution()
                .getOrDefault("CONNECTION_REFUSED", 0));

        // 再加入两台高权重实例：i1 权重占比 1/11，若探测不钉选，几乎不可能被权重选中
        h.registry.register(SVC, "i2", 5, 0);
        h.registry.register(SVC, "i3", 5, 0);
        int i1CallsBefore = h.downstream.invokeCount("i1");
        ForwardOutcome whileOpen = h.forwarder.forward(req("while-open"));
        assertTrue(whileOpen.success());
        assertEquals(i1CallsBefore, h.downstream.invokeCount("i1"), "OPEN 期间不得调用 i1");
        log.info("input: forward while i1 OPEN; result: servedBy={}, i1 circuitSkips={}; "
                        + "basis=OPEN fail-fast, share goes to i2/i3",
                whileOpen.attempts().get(0).instanceId(), viewOf(h, "i1").circuitSkipCount());
        assertTrue(viewOf(h, "i1").circuitSkipCount() >= 1, "熔断跳过必须计入只读视图");

        // 冷却到期：先查两次视图——视图本身绝不能触发探测或改变状态
        h.clock.addAndGet(2000);
        h.forwarder.runtimeView(SVC);
        GatewayForwarder.InstanceView beforeProbe = viewOf(h, "i1");
        log.info("input: cooldown elapsed, runtimeView read twice; result: state={}, probing={}, "
                        + "i1Calls={}; basis=read-only view never mutates circuit state",
                beforeProbe.circuitState(), beforeProbe.probing(), h.downstream.invokeCount("i1"));
        assertEquals(CircuitState.OPEN.name(), beforeProbe.circuitState());
        assertFalse(beforeProbe.probing());
        assertEquals(i1CallsBefore, h.downstream.invokeCount("i1"), "查看视图不得触发探测");

        // i1 恢复健康但响应很慢（阻塞），探测请求会卡在 HALF_OPEN 窗口内
        h.downstream.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        h.downstream.blockOn("i1");
        ExecutorService pool = Executors.newFixedThreadPool(6);
        Future<ForwardOutcome> probeRequest = pool.submit(() -> h.forwarder.forward(req("probe")));
        assertTrue(h.downstream.entered("i1").await(5, TimeUnit.SECONDS),
                "冷却到期后必须真的有一次探测请求到达 i1");
        assertEquals(CircuitState.HALF_OPEN.name(), viewOf(h, "i1").circuitState());
        assertTrue(viewOf(h, "i1").probing(), "探测窗口内视图必须显示正在探测");

        // 探测窗口内并发请求：不得一拥而上，全部绕过 i1 走 i2/i3
        List<Future<ForwardOutcome>> concurrent = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            int seq = i;
            concurrent.add(pool.submit(() -> h.forwarder.forward(req("during-probe-" + seq))));
        }
        for (Future<ForwardOutcome> f : concurrent) {
            ForwardOutcome o = f.get(5, TimeUnit.SECONDS);
            assertTrue(o.success());
            assertFalse(o.attempts().get(0).instanceId().equals("i1"),
                    "探测窗口内其它请求不得打到 i1");
        }
        assertEquals(i1CallsBefore + 1, h.downstream.invokeCount("i1"),
                "同一探测窗口内只能有唯一一次探测到达 i1");
        log.info("input: 5 concurrent forwards during half-open probe; result: all served by i2/i3, "
                + "i1 calls={}; basis=single probe per window, no stampede", h.downstream.invokeCount("i1"));

        // 放行探测：成功后立即恢复接流量
        h.downstream.unblock("i1");
        assertTrue(probeRequest.get(5, TimeUnit.SECONDS).success(), "探测请求本身应成功");
        assertEquals(CircuitState.CLOSED.name(), viewOf(h, "i1").circuitState());
        log.info("input: probe released and succeeded; result: circuit={}; "
                + "basis=trial success closes circuit immediately", viewOf(h, "i1").circuitState());

        boolean i1BackInRotation = false;
        for (int i = 0; i < 30 && !i1BackInRotation; i++) {
            i1BackInRotation = h.forwarder.forward(req("after-" + i))
                    .attempts().get(0).instanceId().equals("i1");
        }
        log.info("input: up to 30 forwards after recovery; result: i1BackInRotation={}; "
                + "basis=recovered instance rejoins weighted distribution", i1BackInRotation);
        assertTrue(i1BackInRotation, "恢复后 i1 必须重新参与权重分发");
        pool.shutdown();
    }

    @Test
    void failedProbeKeepsCircuitOpenAndRestartsCooldown() {
        var h = harness(3, 2000);
        h.registry.register(SVC, "i1");
        h.downstream.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        for (int i = 0; i < 3; i++) {
            h.forwarder.forward(req("f" + i));
        }
        assertEquals(CircuitState.OPEN.name(), viewOf(h, "i1").circuitState());
        int callsAfterOpen = h.downstream.invokeCount("i1");

        // 冷却到期但实例仍故障：探测真实发出且失败 -> 保持熔断并重新计时
        h.clock.addAndGet(2000);
        ForwardOutcome failedProbe = h.forwarder.forward(req("probe-fail"));
        log.info("input: cooldown elapsed, i1 still REFUSE; result: success={}, i1Calls={}, circuit={}; "
                        + "basis=real probe executed, failed probe re-opens with fresh timer",
                failedProbe.success(), h.downstream.invokeCount("i1"), viewOf(h, "i1").circuitState());
        assertFalse(failedProbe.success());
        assertEquals(callsAfterOpen + 1, h.downstream.invokeCount("i1"),
                "冷却到期后必须真的向 i1 发一次探测");
        assertEquals(CircuitState.OPEN.name(), viewOf(h, "i1").circuitState());

        // 重新计时未满月：不再探测
        ForwardOutcome rejected = h.forwarder.forward(req("too-early"));
        assertEquals(FailureType.CIRCUIT_OPEN, rejected.failureType());
        assertEquals(callsAfterOpen + 1, h.downstream.invokeCount("i1"), "重新计时内不得再探测");
        h.clock.addAndGet(1999);
        h.forwarder.forward(req("still-too-early"));
        assertEquals(callsAfterOpen + 1, h.downstream.invokeCount("i1"), "冷却未满不得探测");

        // 再次到期：实例已恢复 -> 探测成功 -> 恢复接流量
        h.downstream.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        h.clock.addAndGet(1);
        ForwardOutcome recovered = h.forwarder.forward(req("probe-ok"));
        log.info("input: second cooldown elapsed, i1 healthy; result: success={}, circuit={}; "
                + "basis=probe success closes circuit", recovered.success(), viewOf(h, "i1").circuitState());
        assertTrue(recovered.success());
        assertEquals(CircuitState.CLOSED.name(), viewOf(h, "i1").circuitState());
    }
}
