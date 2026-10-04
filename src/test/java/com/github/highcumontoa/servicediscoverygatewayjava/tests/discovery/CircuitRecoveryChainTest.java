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

import java.util.List;
import java.util.Map;
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
 * 熔断恢复全链路测试（叠加权重与容量筛选）：
 * - 先熔断 -> 冷却到期 -> 经过权重/容量筛选后仍真实探测 -> 探测成功恢复接流量；
 * - 探测失败保持熔断并重新计时；
 * - 同一探测窗口内并发请求不一拥而上（唯一探测，其余走健康实例）；
 * - 只读状态视图：熔断/探测中/失败类型分布/跳过累计，查看本身不改变状态。
 */
class CircuitRecoveryChainTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitRecoveryChainTest.class);
    private static final String SVC = "svc";

    private record Harness(GatewayForwarder forwarder,
                           SimulatedDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
    }

    private Harness harness(DownstreamHandler downstream, SimulatedDownstream sim) {
        var clock = new AtomicLong(1000);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registry = new InMemoryServiceRegistry(health, downstream, clock::get);
        var forwarder = new GatewayForwarder(registry, new WeightedRoundRobinLoadBalancer(),
                downstream, new RetryPolicy(1, 0),
                new CircuitBreakerConfig(3, 2000, 1),
                clock::get, 50, millis -> { });
        return new Harness(forwarder, sim, registry, clock);
    }

    private GatewayForwarder.InstanceStatusView viewOf(Harness h, String id) {
        return h.forwarder.statusView(SVC).stream()
                .filter(v -> v.instanceId().equals(id)).findFirst().orElseThrow();
    }

    @Test
    void openThenCooldownThenRealProbeThroughWeightCapacityFilterRecovers() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim, sim);
        // i1 高权重、有容量上限；i2 兜底
        h.registry.register(SVC, "i1", 5, 10);
        h.registry.register(SVC, "i2", 1, 0);
        sim.setMode("i1", SimulatedDownstream.Mode.REFUSE);

        // 阶段 1：i1 连续失败（轮询会交替选中两者，只统计 i1 失败）直至熔断
        for (int i = 0; i < 8 && !viewOf(h, "i1").circuitState().equals("OPEN"); i++) {
            h.forwarder.forward(new ForwardRequest(SVC, "open-" + i, "p", false));
        }
        var openView = viewOf(h, "i1");
        log.info("input: i1 REFUSE until circuit opens; result: state={}, failureDist={}, skips={}; "
                        + "basis=threshold 3 reached",
                openView.circuitState(), openView.failureTypeDistribution(), openView.circuitOpenSkips());
        assertEquals("OPEN", openView.circuitState());
        assertEquals(3, openView.failureTypeDistribution().get(FailureType.CONNECTION_REFUSED),
                "失败类型分布必须记录真实失败");

        // 阶段 2：OPEN 期间请求全部绕开 i1（熔断跳过计数增长），i2 承载
        long skipsBefore = viewOf(h, "i1").circuitOpenSkips();
        ForwardOutcome duringOpen = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        assertTrue(duringOpen.success());
        assertEquals("i2", duringOpen.attempts().get(0).instanceId());
        assertTrue(viewOf(h, "i1").circuitOpenSkips() > skipsBefore, "OPEN 期间跳过计数必须累计");

        // 阶段 3：冷却到期 + i1 恢复 -> 经过权重/容量筛选后，i1 必须真实收到一次探测
        h.clock.addAndGet(2000);
        sim.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        int i1CallsBefore = sim.invokeCount("i1");
        ForwardOutcome probe = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        int i1ProbeCalls = sim.invokeCount("i1") - i1CallsBefore;
        var recovered = viewOf(h, "i1");
        log.info("input: cooldown elapsed, i1 NORMAL; result: probeChosen={}, i1ProbeCalls={}, "
                        + "state={}, probing={}; basis=real downstream probe flips HALF_OPEN->CLOSED",
                probe.attempts().get(0).instanceId(), i1ProbeCalls,
                recovered.circuitState(), recovered.probing());
        assertTrue(probe.success());
        assertEquals("i1", probe.attempts().get(0).instanceId(), "冷却到点后探测必须真实打到 i1");
        assertEquals(1, i1ProbeCalls, "探测是对下游的一次真实调用");
        assertEquals("CLOSED", recovered.circuitState(), "探测成功立即恢复");
        assertFalse(recovered.probing());

        // 阶段 4：恢复后 i1 按权重重新承接流量
        int i1After = 0;
        for (int i = 0; i < 12; i++) {
            ForwardOutcome o = h.forwarder.forward(new ForwardRequest(SVC, "post-" + i, "p", false));
            if (o.attempts().get(0).instanceId().equals("i1")) {
                i1After++;
            }
        }
        log.info("input: 12 requests after recovery, weights i1=5 i2=1; result: i1 served={}; "
                + "basis=recovered instance rejoins weighted distribution", i1After);
        assertTrue(i1After > 6, "恢复后高权重的 i1 应承接大部分流量，实际=" + i1After);
    }

    @Test
    void failedProbeKeepsCircuitOpenAndRestartsCooldown() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim, sim);
        h.registry.register(SVC, "i1", 5, 0);
        h.registry.register(SVC, "i2", 1, 0);
        sim.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        for (int i = 0; i < 8 && !viewOf(h, "i1").circuitState().equals("OPEN"); i++) {
            h.forwarder.forward(new ForwardRequest(SVC, "open-" + i, "p", false));
        }
        assertEquals("OPEN", viewOf(h, "i1").circuitState());

        // 冷却到期但故障依旧 -> 探测真实发出并失败 -> 保持 OPEN 重新计时
        h.clock.addAndGet(2000);
        ForwardOutcome probe = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        var afterFailedProbe = viewOf(h, "i1");
        log.info("input: cooldown elapsed but i1 still REFUSE; result: probeSuccess={}, state={}, "
                        + "failureDist={}; basis=failed trial returns to OPEN, timer restarts",
                probe.success(), afterFailedProbe.circuitState(), afterFailedProbe.failureTypeDistribution());
        assertFalse(probe.success() && probe.attempts().get(0).instanceId().equals("i1")
                && probe.attempts().size() > 0 && probe.success());
        assertEquals("OPEN", afterFailedProbe.circuitState(), "探测失败必须保持熔断");

        // 新冷却未到期：请求仍绕开 i1
        ForwardOutcome stillOpen = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        assertEquals("i2", stillOpen.attempts().get(0).instanceId(), "重新计时的冷却期内不得再探测");
        assertEquals("OPEN", viewOf(h, "i1").circuitState());
    }

    @Test
    void statusViewIsReadOnlyAndNeverTriggersProbe() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim, sim);
        h.registry.register(SVC, "i1", 1, 0);
        sim.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        for (int i = 0; i < 3; i++) {
            h.forwarder.forward(new ForwardRequest(SVC, "open-" + i, "p", false));
        }
        assertEquals("OPEN", viewOf(h, "i1").circuitState());

        // 冷却时间已过：反复查看视图不得触发翻牌/探测
        h.clock.addAndGet(5000);
        sim.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        int callsBefore = sim.invokeCount("i1");
        for (int i = 0; i < 5; i++) {
            var v = viewOf(h, "i1");
            assertEquals("OPEN", v.circuitState(), "只读视图不得触发 OPEN->HALF_OPEN 翻牌");
            assertFalse(v.probing());
        }
        int callsAfter = sim.invokeCount("i1");
        log.info("input: cooldown long past, statusView x5; result: i1 downstream delta={}, state={}; "
                + "basis=view is read-only, only real traffic probes",
                callsAfter - callsBefore, viewOf(h, "i1").circuitState());
        assertEquals(callsBefore, callsAfter, "查看视图不得产生任何下游调用");

        // 真实请求到来才翻牌探测并恢复
        ForwardOutcome probe = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        assertTrue(probe.success());
        assertEquals("CLOSED", viewOf(h, "i1").circuitState());
    }

    @Test
    void concurrentRequestsInProbeWindowDoNotStampede() throws Exception {
        var sim = new SimulatedDownstream(0);
        // 包装下游：仅在熔断打开后（gateArmed）阻塞 i1 的探测调用，直到主线程放行
        var probeEntered = new CountDownLatch(1);
        var probeRelease = new CountDownLatch(1);
        var gateArmed = new java.util.concurrent.atomic.AtomicBoolean(false);
        var i1CallsAtGate = new java.util.concurrent.atomic.AtomicInteger(0);
        DownstreamHandler gating = new DownstreamHandler() {
            @Override
            public DownstreamResponse invoke(String instanceId, String payload, long timeoutMillis) {
                if (instanceId.equals("i1") && gateArmed.get()) {
                    i1CallsAtGate.incrementAndGet();
                    probeEntered.countDown();
                    try {
                        probeRelease.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return sim.invoke(instanceId, payload, timeoutMillis);
            }
        };
        var h = harness(gating, sim);
        h.registry.register(SVC, "i1", 5, 0);
        h.registry.register(SVC, "i2", 1, 0);
        sim.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        for (int i = 0; i < 8 && !viewOf(h, "i1").circuitState().equals("OPEN"); i++) {
            h.forwarder.forward(new ForwardRequest(SVC, "open-" + i, "p", false));
        }
        assertEquals("OPEN", viewOf(h, "i1").circuitState());

        h.clock.addAndGet(2000);
        sim.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        gateArmed.set(true); // 此后 i1 的下一次真实调用（即探测）会被阻塞在闸门处

        // 线程 A：触发翻牌并成为唯一探测（阻塞在下游调用里）
        ExecutorService pool = Executors.newFixedThreadPool(7);
        Future<ForwardOutcome> probeFuture = pool.submit(() ->
                h.forwarder.forward(new ForwardRequest(SVC, "probe", "p", false)));
        assertTrue(probeEntered.await(5, TimeUnit.SECONDS), "探测请求必须真实进入下游");
        assertTrue(viewOf(h, "i1").probing(), "探测进行中视图必须可见 probing=true");

        // 探测窗口内并发涌入 6 个请求：不得一拥而上打向 i1，全部走 i2
        var gate = new CountDownLatch(1);
        var futures = new java.util.ArrayList<Future<ForwardOutcome>>();
        for (int i = 0; i < 6; i++) {
            int idx = i;
            futures.add(pool.submit(() -> {
                gate.await();
                return h.forwarder.forward(new ForwardRequest(SVC, "c-" + idx, "p", false));
            }));
        }
        gate.countDown();
        for (var f : futures) {
            ForwardOutcome o = f.get();
            assertTrue(o.success());
            assertEquals("i2", o.attempts().get(0).instanceId(),
                    "探测窗口内其它请求必须走健康实例，不得挤向半开实例");
        }
        int i1DuringProbe = i1CallsAtGate.get();
        log.info("input: 6 concurrent requests while probe in flight; result: i1Calls={}, probing={}; "
                + "basis=exactly one trial per probe window", i1DuringProbe, viewOf(h, "i1").probing());
        assertEquals(1, i1DuringProbe, "同一探测窗口内 i1 只能收到一次探测调用");

        // 放行探测 -> 成功 -> CLOSED
        probeRelease.countDown();
        ForwardOutcome probeOutcome = probeFuture.get(5, TimeUnit.SECONDS);
        pool.shutdown();
        log.info("input: probe released and succeeds; result: success={}, state={}; "
                + "basis=trial success closes circuit", probeOutcome.success(),
                viewOf(h, "i1").circuitState());
        assertTrue(probeOutcome.success());
        assertEquals("CLOSED", viewOf(h, "i1").circuitState());
        assertFalse(viewOf(h, "i1").probing());
    }
}
