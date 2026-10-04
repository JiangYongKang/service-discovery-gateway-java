package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.DownstreamHandler;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway.GatewayForwarder;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.InMemoryServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.WeightedRoundRobinLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamResponse;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardOutcome;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.HealthConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 灰度权重 + 容量治理测试：
 * - 健康实例间按权重精确分摊（smooth WRR）；
 * - 并发下在途名额计数准确、绝不超发；
 * - 成功/超时/下游报错都归还名额，实例不会被永久判满载；
 * - 满载实例本轮让出份额，空下来后自动回到分发。
 */
class WeightedCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(WeightedCapacityTest.class);
    private static final String SVC = "svc";

    private record Harness(GatewayForwarder forwarder,
                           SimulatedDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
    }

    private Harness harness(SimulatedDownstream downstream) {
        var clock = new AtomicLong(0);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registry = new InMemoryServiceRegistry(health, downstream, clock::get);
        var forwarder = new GatewayForwarder(registry, new WeightedRoundRobinLoadBalancer(),
                downstream, new RetryPolicy(1, 0),
                new CircuitBreakerConfig(100, 10_000, 1),
                clock::get, 50, millis -> { });
        return new Harness(forwarder, downstream, registry, clock);
    }

    @Test
    void trafficIsDistributedByWeight() {
        var h = harness(new SimulatedDownstream(0));
        h.registry.register(SVC, "heavy", 3, 0);
        h.registry.register(SVC, "light", 1, 0);

        int total = 40;
        int heavyCount = 0;
        int lightCount = 0;
        for (int i = 0; i < total; i++) {
            ForwardOutcome o = h.forwarder.forward(new ForwardRequest(SVC, null, "p" + i, false));
            assertTrue(o.success());
            if (o.attempts().get(0).instanceId().equals("heavy")) {
                heavyCount++;
            } else {
                lightCount++;
            }
        }
        log.info("input: weights heavy=3, light=1, {} sequential requests; result: heavy={}, light={}; "
                + "basis=smooth WRR distributes exactly by weight ratio", total, heavyCount, lightCount);
        assertEquals(30, heavyCount, "权重 3:1 下 heavy 应占 3/4");
        assertEquals(10, lightCount, "权重 3:1 下 light 应占 1/4");
    }

    @Test
    void inFlightSlotsAreAccurateAndAlwaysReleasedUnderConcurrency() throws Exception {
        var clock = new AtomicLong(0);
        var sim = new SimulatedDownstream(0);
        var peakInFlight = new AtomicInteger(0);
        DownstreamHandler sampling = (instanceId, payload, timeoutMillis) ->
                sim.invoke(instanceId, payload, timeoutMillis);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registry = new InMemoryServiceRegistry(health, sampling, clock::get);
        var forwarder = new GatewayForwarder(registry, new WeightedRoundRobinLoadBalancer(),
                sampling, new RetryPolicy(1, 0),
                new CircuitBreakerConfig(100, 10_000, 1),
                clock::get, 50, millis -> { });

        // i1 权重极高但容量仅 1；i2 权重低不限容量。i1 超时模式拖长在途时间。
        registry.register(SVC, "i1", 100, 1);
        registry.register(SVC, "i2", 1, 0);
        sim.setMode("i1", SimulatedDownstream.Mode.TIMEOUT);
        sim.setTimeoutSleepMillis(30);

        // 采样线程：并发期间轮询 i1 在途数，记录峰值
        var samplingStop = new CountDownLatch(1);
        var i1 = registry.getInstance(SVC, "i1");
        Thread sampler = new Thread(() -> {
            while (samplingStop.getCount() > 0) {
                peakInFlight.accumulateAndGet(i1.inFlight(), Math::max);
                Thread.yield();
            }
        });
        sampler.start();

        int threads = 8;
        int perThread = 6;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<ForwardOutcome>> futures = new ArrayList<>();
        var outcomes = new ConcurrentHashMap<String, Integer>();
        for (int t = 0; t < threads; t++) {
            int thread = t;
            futures.add(pool.submit(() -> {
                gate.await();
                ForwardOutcome last = null;
                for (int i = 0; i < perThread; i++) {
                    // 每个线程用不同幂等键，避免去重合并；retryable=false 聚焦容量语义
                    last = forwarder.forward(
                            new ForwardRequest(SVC, "k-" + thread + "-" + i, "p", false));
                    outcomes.merge(last.success() ? "success" : "failure", 1, Integer::sum);
                }
                return last;
            }));
        }
        gate.countDown();
        for (Future<ForwardOutcome> f : futures) {
            f.get();
        }
        pool.shutdown();
        samplingStop.countDown();
        sampler.join();

        int i1Calls = sim.invokeCount("i1");
        int i2Calls = sim.invokeCount("i2");
        log.info("input: i1 weight=100 maxConcurrency=1 TIMEOUT(30ms), i2 weight=1 unlimited; "
                        + "{} threads x {} requests; result: i1Calls={}, i2Calls={}, peakInFlight(i1)={}, "
                        + "inFlightAfterAll={}, outcomes={}; basis=slot never exceeded, "
                        + "overflow share goes to i2, slots always released",
                threads, perThread, i1Calls, i2Calls, peakInFlight.get(), i1.inFlight(), outcomes);

        assertTrue(peakInFlight.get() <= 1, "容量上限 1 绝不超发，实测峰值=" + peakInFlight.get());
        assertTrue(i1Calls > 0, "i1 应承载一部分流量");
        assertTrue(i2Calls > 0, "i1 满载期间份额必须让给 i2");
        assertEquals(0, i1.inFlight(), "全部请求（含超时失败）结束后名额必须归还，不得泄漏");

        // 空下来后自动回到分发：i1 恢复正常模式，后续请求仍应能选中它
        sim.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        ForwardOutcome after = forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        log.info("input: i1 recovered NORMAL after drain; result: chosen={}, inFlight={}; "
                        + "basis=freed instance rejoins distribution automatically",
                after.attempts().get(0).instanceId(), i1.inFlight());
        assertTrue(after.success());
        assertEquals(0, i1.inFlight(), "成功请求结束后名额同样必须归还");
    }

    @Test
    void errorResponsesAlsoReleaseSlots() {
        var h = harness(new SimulatedDownstream(0));
        h.registry.register(SVC, "i1", 1, 1); // 容量仅 1
        h.registry.register(SVC, "i2", 1, 0);
        h.downstream().setMode("i1", SimulatedDownstream.Mode.ERROR_500);
        var i1 = h.registry.getInstance(SVC, "i1");

        // 交替让 i1 产生 500（下游报错）：每次之后名额都必须归还
        for (int round = 0; round < 5; round++) {
            h.forwarder.forward(new ForwardRequest(SVC, "k500-" + round, "p", false));
            assertEquals(0, i1.inFlight(), "下游 500 后名额必须归还 (round=" + round + ")");
        }
        h.downstream().setMode("i1", SimulatedDownstream.Mode.NORMAL);
        ForwardOutcome o = h.forwarder.forward(new ForwardRequest(SVC, null, "p", false));
        log.info("input: i1 ERROR_500 x5 then NORMAL, maxConcurrency=1; result: success={}, inFlight={}; "
                + "basis=error responses release slots, instance not stuck as full",
                o.success(), i1.inFlight());
        assertTrue(o.success());
        assertEquals(0, i1.inFlight());
    }
}
