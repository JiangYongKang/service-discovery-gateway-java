package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.DownstreamHandler;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway.GatewayForwarder;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.InMemoryServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.WeightedRoundRobinLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardOutcome;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.HealthConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 熔断恢复扫描 / 在线权重调整 / 失败分布最近口径 的回归测试。
 *
 * 覆盖：
 * - 多个实例冷却同时到期时，每个实例都恰好收到一次真实探测并各自恢复；
 * - 探测失败后重新计时，新冷却期内不再探测；
 * - 权重极低的实例冷却到点后，依然被恢复扫描真实探测到（与权重无关）；
 * - 调整已存在实例的权重后，分流比例立即按新值变化；
 * - 失败类型分布是最近一段时间口径，旧计数随窗口滑出而消失。
 */
class RecoveryProbeSweepTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryProbeSweepTest.class);
    private static final String SVC = "svc";

    private record Harness(GatewayForwarder forwarder,
                           SimulatedDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
    }

    /** 同步执行器：恢复探测在调用线程内完成，扫描返回时结果已落地，断言确定。 */
    private Harness harness(SimulatedDownstream sim) {
        var clock = new AtomicLong(1000);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registry = new InMemoryServiceRegistry(health, sim, clock::get);
        var forwarder = new GatewayForwarder(registry, new WeightedRoundRobinLoadBalancer(),
                sim, new RetryPolicy(1, 0),
                new CircuitBreakerConfig(3, 2000, 1),
                clock::get, 50, millis -> { },
                new com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry
                        .SlidingWindowRetryBudget(
                        com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                                .RetryBudgetPolicy.disabled()),
                Runnable::run);
        return new Harness(forwarder, sim, registry, clock);
    }

    private void openCircuit(Harness h, String id) {
        for (int i = 0; i < 20; i++) {
            if ("OPEN".equals(stateOf(h, id))) {
                return;
            }
            h.forwarder().forward(new ForwardRequest(SVC, "open-" + id + "-" + i, "p", false));
        }
        assertEquals("OPEN", stateOf(h, id), "实例 " + id + " 应已熔断");
    }

    private String stateOf(Harness h, String id) {
        return h.forwarder().statusView(SVC).stream()
                .filter(v -> v.instanceId().equals(id)).findFirst().orElseThrow().circuitState();
    }

    @Test
    void multipleInstancesCooldownExpireEachProbedExactlyOnceAndRecover() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim);
        List<String> ids = List.of("i1", "i2", "i3");
        ids.forEach(id -> h.registry().register(SVC, id, 1, 0));
        ids.forEach(id -> sim.setMode(id, SimulatedDownstream.Mode.REFUSE));

        // 抖动期：三个实例一起被熔断
        ids.forEach(id -> openCircuit(h, id));
        log.info("input: 3 instances all REFUSE; result: states={}; basis=whole downstream jitter "
                + "opens every circuit", ids.stream().map(id -> id + "=" + stateOf(h, id)).toList());

        // 下游恢复，冷却同时到点
        ids.forEach(id -> sim.setMode(id, SimulatedDownstream.Mode.NORMAL));
        h.clock().addAndGet(2000);
        var before = new java.util.HashMap<String, Integer>();
        ids.forEach(id -> before.put(id, sim.invokeCount(id)));

        int dispatched = h.forwarder().sweepRecoveryProbes(SVC);
        var deltas = new java.util.HashMap<String, Integer>();
        ids.forEach(id -> deltas.put(id, sim.invokeCount(id) - before.get(id)));
        var states = ids.stream().map(id -> id + "=" + stateOf(h, id)).toList();
        log.info("input: cooldown expired together, one recovery sweep; result: dispatched={}, "
                        + "probeDeltas={}, states={}; basis=each due instance independently gets "
                        + "exactly one real probe and recovers", dispatched, deltas, states);

        assertEquals(3, dispatched, "三个到期实例都应被派出探测");
        for (String id : ids) {
            assertEquals(1, deltas.get(id), "实例 " + id + " 必须恰好收到一次真实探测");
            assertEquals("CLOSED", stateOf(h, id), "探测成功后实例 " + id + " 必须立即恢复接流量");
        }

        // 紧接着再扫一轮：全部 CLOSED，不应再有任何探测
        int secondDispatch = h.forwarder().sweepRecoveryProbes(SVC);
        log.info("input: second sweep right after recovery; result: dispatched={}; "
                + "basis=closed circuits need no probe", secondDispatch);
        assertEquals(0, secondDispatch, "恢复后不应重复探测");
    }

    @Test
    void failedRecoveryProbeRestartsCooldownAndNoProbeWithinNewWindow() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim);
        h.registry().register(SVC, "i1", 1, 0);
        sim.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        openCircuit(h, "i1");

        // 冷却到期但故障依旧：探测真实发出并失败 -> 回到 OPEN 重新计时
        h.clock().addAndGet(2000);
        int callsBeforeSweep1 = sim.invokeCount("i1");
        int dispatched1 = h.forwarder().sweepRecoveryProbes(SVC);
        int probeCalls1 = sim.invokeCount("i1") - callsBeforeSweep1;
        log.info("input: cooldown expired but still REFUSE; result: dispatched={}, realProbeCalls={}, "
                        + "state={}; basis=failed trial -> OPEN with restarted timer",
                dispatched1, probeCalls1, stateOf(h, "i1"));
        assertEquals(1, dispatched1);
        assertEquals(1, probeCalls1, "必须是一次真实下游探测");
        assertEquals("OPEN", stateOf(h, "i1"), "探测失败必须保持熔断并重新计时");

        // 新冷却期内（差 1ms 才到）：不再派出任何探测
        h.clock().addAndGet(1999);
        int callsMidWindow = sim.invokeCount("i1");
        int dispatchedMid = h.forwarder().sweepRecoveryProbes(SVC);
        log.info("input: 1999ms into restarted cooldown (<2000ms); result: dispatched={}, "
                        + "extraCalls={}, state={}; basis=new cooldown must fully elapse",
                dispatchedMid, sim.invokeCount("i1") - callsMidWindow, stateOf(h, "i1"));
        assertEquals(0, dispatchedMid, "重新计时的冷却期内不得再探测");
        assertEquals(0, sim.invokeCount("i1") - callsMidWindow, "冷却期内不得有真实下游调用");
        assertEquals("OPEN", stateOf(h, "i1"));

        // 新冷却到点：再次探测一次（此时下游仍故障，继续熔断）
        h.clock().addAndGet(1);
        int callsAtDeadline = sim.invokeCount("i1");
        int dispatched2 = h.forwarder().sweepRecoveryProbes(SVC);
        log.info("input: restarted cooldown elapsed; result: dispatched={}, realProbeCalls={}, "
                        + "state={}; basis=one new trial only after fresh cooldown",
                dispatched2, sim.invokeCount("i1") - callsAtDeadline, stateOf(h, "i1"));
        assertEquals(1, dispatched2, "新一轮冷却到点后应再探测一次");
        assertEquals(1, sim.invokeCount("i1") - callsAtDeadline);
        assertEquals("OPEN", stateOf(h, "i1"));
    }

    @Test
    void veryLowWeightInstanceStillProbedWhenCooldownExpires() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim);
        // i1 权重极低，i2 权重极高：正常流量几乎永远不会选中 i1
        h.registry().register(SVC, "i1", 1, 0);
        h.registry().register(SVC, "i2", 1000, 0);
        // i1 在历史抖动中已被熔断（低权重下业务流量本来就很少选中它）
        h.forwarder().forceOpen(SVC, "i1");
        sim.setMode("i1", SimulatedDownstream.Mode.REFUSE);
        sim.setMode("i2", SimulatedDownstream.Mode.NORMAL);
        assertEquals("OPEN", stateOf(h, "i1"));

        // 熔断期间正常流量全部由 i2 承担，i1 收不到任何业务流量
        int i1CallsDuringOpen = sim.invokeCount("i1");
        for (int i = 0; i < 30; i++) {
            ForwardOutcome o = h.forwarder().forward(new ForwardRequest(SVC, "traffic-" + i, "p", false));
            assertTrue(o.success());
            assertEquals("i2", o.attempts().get(0).instanceId());
        }
        assertEquals(i1CallsDuringOpen, sim.invokeCount("i1"), "OPEN 期低权重实例不应收到业务流量");

        // 冷却到点、i1 恢复：即使没有业务流量选中它，恢复扫描也必须真实探测它一次
        h.clock().addAndGet(2000);
        sim.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        int callsBeforeSweep = sim.invokeCount("i1");
        int dispatched = h.forwarder().sweepRecoveryProbes(SVC);
        int probeCalls = sim.invokeCount("i1") - callsBeforeSweep;
        log.info("input: i1 weight=1 vs i2 weight=1000, cooldown expired; result: dispatched={}, "
                        + "i1ProbeCalls={}, state={}; basis=recovery probe is weight-independent",
                dispatched, probeCalls, stateOf(h, "i1"));
        assertEquals(1, dispatched);
        assertEquals(1, probeCalls, "低权重实例冷却到点也必须被真实探测");
        assertEquals("CLOSED", stateOf(h, "i1"), "探测成功立即恢复");
    }

    @Test
    void updatingExistingInstanceWeightChangesDistributionImmediately() {
        var sim = new SimulatedDownstream(0);
        var h = harness(sim);
        // 初始两个实例都注册为权重 1
        h.registry().register(SVC, "a", 1, 0);
        h.registry().register(SVC, "b", 1, 0);

        int aBefore = 0;
        for (int i = 0; i < 20; i++) {
            ForwardOutcome o = h.forwarder().forward(new ForwardRequest(SVC, "before-" + i, "p", false));
            if (o.attempts().get(0).instanceId().equals("a")) {
                aBefore++;
            }
        }
        log.info("input: weights a=1 b=1, 20 requests; result: a={}, b={}; basis=equal weights",
                aBefore, 20 - aBefore);
        assertEquals(10, aBefore, "初始权重 1:1 应均摊");

        // 对“已存在”的实例 a 在线调大权重到 5（而非重新注册）
        var result = h.registry().registerOrUpdate(SVC, "a", 5, 0);
        assertEquals(com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance
                .ServiceRegistry.RegistrationOutcome.UPDATED, result.outcome());
        assertEquals(5, result.effectiveWeight(), "回显必须是真正生效的新权重");
        assertEquals(5, h.registry().getInstance(SVC, "a").weight(), "实例实际权重必须已更新");

        int aAfter = 0;
        int total = 60;
        for (int i = 0; i < total; i++) {
            ForwardOutcome o = h.forwarder().forward(new ForwardRequest(SVC, "after-" + i, "p", false));
            if (o.attempts().get(0).instanceId().equals("a")) {
                aAfter++;
            }
        }
        log.info("input: existing instance a updated weight 1 -> 5 (b stays 1), {} requests; "
                        + "result: a={}, b={}; basis=next rounds distribute by new weight 5:1",
                total, aAfter, total - aAfter);
        assertTrue(aAfter >= 45, "新权重 5:1 下 a 应承接约 5/6 流量，实际 a=" + aAfter);
        assertTrue(total - aAfter <= 15, "b 只应承接约 1/6，实际 b=" + (total - aAfter));
    }

    @Test
    void failureTypeDistributionIsRecentWindowNotLifetimeCumulative() {
        // 窗口 10 秒；阈值很大，避免失败计数触发熔断，专注验证统计口径
        var clock = new AtomicLong(0);
        var cb = new com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit
                .DefaultCircuitBreaker("i1",
                new CircuitBreakerConfig(100, 100_000, 1, 100_000, 10_000),
                clock::get);

        // t=0..1.8s：4 次 CONNECTION_REFUSED + 1 次 TIMEOUT（时钟随真实时间推进）
        clock.set(0);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.CONNECTION_REFUSED);
        clock.set(500);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.CONNECTION_REFUSED);
        clock.set(1_000);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.TIMEOUT);
        clock.set(1_500);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.CONNECTION_REFUSED);
        clock.set(1_800);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.CONNECTION_REFUSED);

        clock.set(5_000);
        var at5s = cb.failureTypeDistribution(clock.get());
        log.info("input: 4 REFUSED + 1 TIMEOUT within [0s,1.8s], view at t=5s; result={}; "
                + "basis=all within last 10s", at5s);
        assertEquals(4, at5s.get(com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.CONNECTION_REFUSED));
        assertEquals(1, at5s.get(com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.TIMEOUT));

        // 反复只读查看不改变任何计数
        for (int i = 0; i < 5; i++) {
            cb.failureTypeDistribution(clock.get());
        }
        assertEquals(5, cb.failureTypeDistribution(clock.get()).values().stream()
                .mapToInt(Integer::intValue).sum(), "只读视图不得改变状态");

        // 时间推进到 t=12s：旧失败全部滑出 10 秒窗口——视图只做时间过滤，旧计数不再出现
        clock.set(12_000);
        var agedOut = cb.failureTypeDistribution(clock.get());
        log.info("input: view at t=12s (no new failures); result={}; "
                + "basis=old failures aged out of the recent window, not cumulative", agedOut);
        assertTrue(agedOut.isEmpty(), "超过窗口的历史失败必须从视图消失，而不是只增不减");
        // 只读查看本身不改变状态：回到窗口内时刻仍能看到原计数
        clock.set(5_000);
        assertEquals(5, cb.failureTypeDistribution(clock.get()).values().stream()
                .mapToInt(Integer::intValue).sum(), "只读视图不得清理或改变内部状态");

        // 最近再发生 2 次 DOWNSTREAM_ERROR
        clock.set(11_500);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.DOWNSTREAM_ERROR);
        clock.set(11_800);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.DOWNSTREAM_ERROR);
        clock.set(12_000);
        var recent = cb.failureTypeDistribution(clock.get());
        log.info("input: 2 recent DOWNSTREAM_ERROR at 11.5s/11.8s, view at t=12s; result={}; "
                + "basis=distribution reflects only the recent window", recent);
        assertEquals(2, recent.get(com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.DOWNSTREAM_ERROR), "最近窗口只反映新失败");
        assertNull(recent.get(com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.CONNECTION_REFUSED), "旧的 REFUSED 计数不应再出现");
        assertNull(recent.get(com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                .FailureType.TIMEOUT), "旧的 TIMEOUT 计数不应再出现");
    }

    @Test
    void concurrentSweepAndTrafficSendOnlyOneProbePerInstanceWindow() throws Exception {
        var sim = new SimulatedDownstream(0);
        // 包装下游：i1 第一次调用（探测）阻塞在闸门处，期间并发扫描与流量都不能再打 i1
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var armed = new java.util.concurrent.atomic.AtomicBoolean(false);
        var inFlightProbes = new java.util.concurrent.atomic.AtomicInteger(0);
        var maxConcurrent = new java.util.concurrent.atomic.AtomicInteger(0);
        DownstreamHandler gating = new DownstreamHandler() {
            @Override
            public com.github.highcumontoa.servicediscoverygatewayjava.discovery.model
                    .DownstreamResponse invoke(String instanceId, String payload, long timeoutMillis) {
                if (instanceId.equals("i1") && armed.get()) {
                    int live = inFlightProbes.incrementAndGet();
                    maxConcurrent.accumulateAndGet(live, Math::max);
                    entered.countDown();
                    try {
                        release.await(5, java.util.concurrent.TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    inFlightProbes.decrementAndGet();
                }
                return sim.invoke(instanceId, payload, timeoutMillis);
            }
        };
        var clock = new AtomicLong(1000);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registry = new InMemoryServiceRegistry(health, gating, clock::get);
        // 用真实缓存线程池执行恢复探测，才能与流量线程真正并发
        var pool = java.util.concurrent.Executors.newCachedThreadPool();
        var forwarder = new GatewayForwarder(registry, new WeightedRoundRobinLoadBalancer(),
                gating, new RetryPolicy(1, 0),
                new CircuitBreakerConfig(3, 2000, 1),
                clock::get, 50, millis -> { },
                new com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry
                        .SlidingWindowRetryBudget(com.github.highcumontoa.servicediscoverygatewayjava
                        .discovery.model.RetryBudgetPolicy.disabled()),
                pool);
        registry.register(SVC, "i1", 1, 0);
        registry.register(SVC, "i2", 100, 0);
        forwarder.forceOpen(SVC, "i1");
        sim.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        clock.addAndGet(2000);
        armed.set(true);

        // 后台恢复扫描率先领取唯一探测，探测真实进入下游并阻塞
        var sweepFuture = pool.submit(() -> forwarder.sweepRecoveryProbes(SVC));
        assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS), "探测必须真实进入下游");
        assertEquals(1, sweepFuture.get(), "本轮只有 i1 到期，应派出 1 个探测");
        var probing = forwarder.statusView(SVC).stream()
                .filter(v -> v.instanceId().equals("i1")).findFirst().orElseThrow();
        assertTrue(probing.probing(), "探测在途时视图 probing=true");

        // 探测窗口内：再扫一轮 + 8 个并发业务请求，全部不得再向 i1 发探测（业务走 i2）
        int secondSweep = forwarder.sweepRecoveryProbes(SVC);
        var futures = new java.util.ArrayList<java.util.concurrent.Future<ForwardOutcome>>();
        var gate = new java.util.concurrent.CountDownLatch(1);
        for (int i = 0; i < 8; i++) {
            int idx = i;
            futures.add(pool.submit(() -> {
                gate.await();
                return forwarder.forward(new ForwardRequest(SVC, "c-" + idx, "p", false));
            }));
        }
        gate.countDown();
        for (var f : futures) {
            ForwardOutcome o = f.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(o.success());
            assertEquals("i2", o.attempts().get(0).instanceId(), "探测窗口内业务流量必须绕开 i1");
        }
        int maxLive = maxConcurrent.get();
        log.info("input: probe in flight + concurrent sweep x1 + 8 traffic requests; result: "
                        + "secondSweepDispatched={}, maxConcurrentDownstreamCallsToProbedInstance={}; "
                        + "basis=single-flight claim per instance window", secondSweep, maxLive);
        assertEquals(0, secondSweep, "探测窗口内重复扫描不得再领取探测");
        assertEquals(1, maxLive, "同一实例探测窗口内下游真实调用并发度必须恰好为 1");

        release.countDown();
        pool.shutdown();
        // 等待被放行的真实探测在恢复线程内落地（成功 -> CLOSED），最多等 5 秒
        long deadline = System.currentTimeMillis() + 5000;
        String finalState;
        do {
            finalState = forwarder.statusView(SVC).stream()
                    .filter(v -> v.instanceId().equals("i1")).findFirst().orElseThrow().circuitState();
            if (!"HALF_OPEN".equals(finalState)) {
                break;
            }
            Thread.sleep(10);
        } while (System.currentTimeMillis() < deadline);
        assertEquals("CLOSED", finalState, "探测成功后恢复 CLOSED");
    }
}
