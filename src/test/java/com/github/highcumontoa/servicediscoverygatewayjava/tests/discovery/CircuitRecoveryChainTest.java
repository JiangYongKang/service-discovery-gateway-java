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

    @Test
    void multipleInstancesCoolingDownTogetherEachGetExactlyOneProbeAndRecoverIndependently() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim, sim);
        // 下游整体抖动：三个实例同时故障
        h.registry.register(SVC, "i1", 1, 0);
        h.registry.register(SVC, "i2", 1, 0);
        h.registry.register(SVC, "i3", 1, 0);
        for (String id : List.of("i1", "i2", "i3")) {
            sim.setMode(id, SimulatedDownstream.Mode.REFUSE);
        }

        // 阶段 1：流量把三个实例的熔断器全部打开
        boolean allOpen = false;
        for (int i = 0; i < 40 && !allOpen; i++) {
            h.forwarder.forward(new ForwardRequest(SVC, "open-" + i, "p", false));
            allOpen = viewOf(h, "i1").circuitState().equals("OPEN")
                    && viewOf(h, "i2").circuitState().equals("OPEN")
                    && viewOf(h, "i3").circuitState().equals("OPEN");
        }
        assertTrue(allOpen, "三个实例应全部熔断");
        log.info("input: all 3 instances REFUSE; result: all circuits OPEN; "
                + "basis=downstream-wide outage opens every instance circuit");

        // 阶段 2：冷却同时到期 + 下游整体恢复 -> 每个实例都必须恰好收到一次真实探测
        h.clock.addAndGet(2000);
        for (String id : List.of("i1", "i2", "i3")) {
            sim.setMode(id, SimulatedDownstream.Mode.NORMAL);
        }
        Map<String, Integer> callsBefore = Map.of(
                "i1", sim.invokeCount("i1"), "i2", sim.invokeCount("i2"), "i3", sim.invokeCount("i3"));
        for (int r = 0; r < 3; r++) {
            ForwardOutcome o = h.forwarder.forward(new ForwardRequest(SVC, "recover-" + r, "p", false));
            assertTrue(o.success(), "恢复期每个请求都应成功");
        }
        Map<String, Integer> probeCalls = new java.util.HashMap<>();
        for (String id : List.of("i1", "i2", "i3")) {
            probeCalls.put(id, sim.invokeCount(id) - callsBefore.get(id));
        }
        var states = List.of(viewOf(h, "i1").circuitState(), viewOf(h, "i2").circuitState(),
                viewOf(h, "i3").circuitState());
        log.info("input: 3 instances cooldown elapsed together, downstream recovered, 3 requests; "
                        + "result: probeCallsPerInstance={}, states={}; "
                        + "basis=each instance gets exactly ONE real probe and recovers independently",
                probeCalls, states);
        assertEquals(Map.of("i1", 1, "i2", 1, "i3", 1), probeCalls,
                "每个冷却到点的实例都必须恰好收到一次真实探测，不得有实例卡在半开等不到探测");
        assertEquals(List.of("CLOSED", "CLOSED", "CLOSED"), states,
                "探测成功后各实例各自恢复接流量，互不影响");
        assertFalse(viewOf(h, "i1").probing() || viewOf(h, "i2").probing()
                || viewOf(h, "i3").probing(), "恢复后不得有实例停在探测中状态");

        // 阶段 3：全部恢复后流量重新分摊到三个实例
        var served = new java.util.HashMap<String, Integer>();
        for (int i = 0; i < 30; i++) {
            ForwardOutcome o = h.forwarder.forward(new ForwardRequest(SVC, "post-" + i, "p", false));
            served.merge(o.attempts().get(0).instanceId(), 1, Integer::sum);
        }
        log.info("input: 30 requests after full recovery; result: served={}; "
                + "basis=all recovered instances rejoin traffic", served);
        assertEquals(3, served.size(), "恢复后三个实例都应重新承接流量");
    }

    @Test
    void lowWeightInstanceStillGetsRealProbeWhenCooldownElapses() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim, sim);
        // i1 权重 100 健康兜底；i2 权重仅 1 且被熔断
        h.registry.register(SVC, "i1", 100, 0);
        h.registry.register(SVC, "i2", 1, 0);
        h.forwarder.forceOpen(SVC, "i2");
        assertEquals("OPEN", viewOf(h, "i2").circuitState());

        // 冷却到点：即使 i2 权重极低，下一个请求也必须真实探测 i2 而不是按权重走 i1
        h.clock.addAndGet(2000);
        int i2CallsBefore = sim.invokeCount("i2");
        ForwardOutcome probe = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        var view = viewOf(h, "i2");
        log.info("input: i2 weight=1 vs i1 weight=100, i2 cooldown elapsed; result: chosen={}, "
                        + "i2ProbeCalls={}, state={}; basis=probe priority ignores weight",
                probe.attempts().get(0).instanceId(), sim.invokeCount("i2") - i2CallsBefore,
                view.circuitState());
        assertTrue(probe.success());
        assertEquals("i2", probe.attempts().get(0).instanceId(),
                "低权重实例冷却到点也必须被真实探测，不得被高权重实例挤掉");
        assertEquals(1, sim.invokeCount("i2") - i2CallsBefore, "探测是对 i2 的一次真实下游调用");
        assertEquals("CLOSED", view.circuitState(), "探测成功立即恢复");
    }

    @Test
    void failedProbeRestartsCooldownAndNoProbeDuringNewCooldown() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim, sim);
        h.registry.register(SVC, "i1", 1, 0);
        h.registry.register(SVC, "i2", 1, 0);
        sim.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        for (int i = 0; i < 8 && !viewOf(h, "i1").circuitState().equals("OPEN"); i++) {
            h.forwarder.forward(new ForwardRequest(SVC, "open-" + i, "p", false));
        }
        assertEquals("OPEN", viewOf(h, "i1").circuitState());

        // 冷却到期但故障依旧：探测真实发出并失败 -> 重新计时
        h.clock.addAndGet(2000);
        ForwardOutcome failedProbe = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        assertEquals("i1", failedProbe.attempts().get(0).instanceId(), "冷却到点必须真实探测 i1");
        assertEquals("OPEN", viewOf(h, "i1").circuitState(), "探测失败保持熔断并重新计时");

        // 新冷却期内：再多请求也不得对 i1 发起任何新的下游调用
        int i1CallsAfterFailedProbe = sim.invokeCount("i1");
        h.clock.addAndGet(1999); // 新冷却 2000ms 未满
        for (int i = 0; i < 5; i++) {
            ForwardOutcome o = h.forwarder.forward(new ForwardRequest(SVC, "during-" + i, "p", false));
            assertTrue(o.success());
            assertEquals("i2", o.attempts().get(0).instanceId(), "新冷却期内请求必须绕开 i1");
        }
        int i1Delta = sim.invokeCount("i1") - i1CallsAfterFailedProbe;
        log.info("input: 5 requests within restarted cooldown (1999ms<2000ms); result: i1NewCalls={}, "
                        + "state={}; basis=failed probe restarts timer, no probe during new cooldown",
                i1Delta, viewOf(h, "i1").circuitState());
        assertEquals(0, i1Delta, "探测失败重新计时后，新冷却期内不得再探测 i1");

        // 新冷却到期且下游恢复：i1 再次被真实探测并恢复
        h.clock.addAndGet(1);
        sim.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        ForwardOutcome recovered = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        log.info("input: new cooldown elapsed, i1 NORMAL; result: chosen={}, state={}; "
                        + "basis=next probe after restarted cooldown recovers",
                recovered.attempts().get(0).instanceId(), viewOf(h, "i1").circuitState());
        assertEquals("i1", recovered.attempts().get(0).instanceId());
        assertEquals("CLOSED", viewOf(h, "i1").circuitState());
    }

    @Test
    void failureTypeDistributionReflectsOnlyRecentWindow() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim, sim);
        h.registry.register(SVC, "i1", 1, 0);
        h.registry.register(SVC, "i2", 1, 0);
        sim.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        for (int i = 0; i < 8 && !viewOf(h, "i1").circuitState().equals("OPEN"); i++) {
            h.forwarder.forward(new ForwardRequest(SVC, "open-" + i, "p", false));
        }
        var openView = viewOf(h, "i1");
        assertEquals(3, openView.failureTypeDistribution().get(FailureType.CONNECTION_REFUSED),
                "窗口内应看到最近的 3 次拒绝失败");

        // 时钟越过统计窗口（默认 60s）：旧失败不再计入分布，且查看视图不改变任何状态
        h.clock.addAndGet(61_000);
        var staleView = viewOf(h, "i1");
        log.info("input: clock advanced 61s beyond failure window; result: failureDist={}, state={}; "
                        + "basis=distribution reflects recent window only, view stays read-only",
                staleView.failureTypeDistribution(), staleView.circuitState());
        assertTrue(staleView.failureTypeDistribution().isEmpty(),
                "窗口外的历史失败不得再计入分布（不是只增累计）");
        assertEquals("OPEN", staleView.circuitState(), "查看视图不得改变熔断状态");
        assertFalse(staleView.probing());

        // 窗口内出现新失败：分布只包含新失败，旧数据不复活
        ForwardOutcome o = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        assertEquals("i1", o.attempts().get(0).instanceId(), "冷却早已到期，本次为真实探测");
        var freshView = viewOf(h, "i1");
        log.info("input: new probe failure inside window; result: failureDist={}; "
                + "basis=only in-window failures are counted", freshView.failureTypeDistribution());
        assertEquals(1, freshView.failureTypeDistribution().get(FailureType.CONNECTION_REFUSED),
                "分布只统计最近窗口内的失败");
    }
}
