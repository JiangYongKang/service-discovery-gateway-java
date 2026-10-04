package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.DownstreamHandler;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway.GatewayForwarder;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.InMemoryServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.WeightedRoundRobinLoadBalancer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 灰度权重与容量治理测试：
 * - 健康实例间按权重精确分摊（并发下比例依然准确）；
 * - 在途名额并发获取不超限，成功/超时/下游报错都如数归还，实例不会被永久判成满载；
 * - 满载实例本轮被跳过、份额让给有余量的实例，空下来后自动回到分发。
 */
class WeightAndCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(WeightAndCapacityTest.class);
    private static final String SVC = "svc";

    /** 可阻塞、可观测在途峰值的下游：invoke 进入时记录该实例当前在途数。 */
    private static class LatchDownstream implements DownstreamHandler {
        private final SimulatedDownstream delegate = new SimulatedDownstream(0);
        private final Function<String, ServiceInstance> instances;
        private final Map<String, CountDownLatch> blockLatches = new ConcurrentHashMap<>();
        private final Map<String, CountDownLatch> enteredLatches = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> maxObservedInFlight = new ConcurrentHashMap<>();

        LatchDownstream(Function<String, ServiceInstance> instances) {
            this.instances = instances;
        }

        void blockOn(String instanceId, CountDownLatch entered, CountDownLatch release) {
            enteredLatches.put(instanceId, entered);
            blockLatches.put(instanceId, release);
        }

        void unblock(String instanceId) {
            CountDownLatch latch = blockLatches.remove(instanceId);
            if (latch != null) {
                latch.countDown();
            }
        }

        int maxInFlightOf(String instanceId) {
            return maxObservedInFlight.getOrDefault(instanceId, new AtomicInteger()).get();
        }

        @Override
        public DownstreamResponse invoke(String instanceId, String payload, long timeoutMillis) {
            ServiceInstance instance = instances.apply(instanceId);
            if (instance != null) {
                maxObservedInFlight.computeIfAbsent(instanceId, k -> new AtomicInteger())
                        .accumulateAndGet(instance.inFlight(), Math::max);
            }
            CountDownLatch entered = enteredLatches.get(instanceId);
            if (entered != null) {
                entered.countDown();
            }
            CountDownLatch block = blockLatches.get(instanceId);
            if (block != null) {
                try {
                    block.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return delegate.invoke(instanceId, payload, timeoutMillis);
        }
    }

    private record Harness(GatewayForwarder forwarder,
                           LatchDownstream downstream,
                           InMemoryServiceRegistry registry,
                           AtomicLong clock) {
        ServiceInstance instance(String id) {
            return registry.getInstance(SVC, id);
        }
    }

    private Harness harness() {
        var clock = new AtomicLong(1000);
        var health = new HealthConfig(3, 1000, 100, 50);
        var registryRef = new InMemoryServiceRegistry[1];
        var downstream = new LatchDownstream(id -> registryRef[0].getInstance(SVC, id));
        var registry = new InMemoryServiceRegistry(health, downstream, clock::get);
        registryRef[0] = registry;
        var forwarder = new GatewayForwarder(registry, new WeightedRoundRobinLoadBalancer(), downstream,
                new RetryPolicy(1, 0), // 聚焦分发与容量：关闭自动重试，每请求恰好一次选择
                new CircuitBreakerConfig(1000, 60_000, 1),
                clock::get, 50, millis -> { });
        return new Harness(forwarder, downstream, registry, clock);
    }

    private static ForwardRequest req(String payload) {
        return new ForwardRequest(SVC, null, payload, true);
    }

    @Test
    void weightedDistributionIsExactUnderConcurrency() throws Exception {
        var h = harness();
        h.registry.register(SVC, "i1", 3, 0);
        h.registry.register(SVC, "i2", 2, 0);
        h.registry.register(SVC, "i3", 1, 0);
        log.info("input: instances i1(w=3), i2(w=2), i3(w=1); 4 threads x 30 forwards = 120");

        int threads = 4;
        int perThread = 30;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();
        for (int t = 0; t < threads; t++) {
            int base = t * perThread;
            futures.add(pool.submit(() -> {
                gate.await();
                for (int i = 0; i < perThread; i++) {
                    ForwardOutcome o = h.forwarder.forward(req("p" + (base + i)));
                    assertTrue(o.success());
                    hits.computeIfAbsent(o.attempts().get(0).instanceId(),
                            k -> new AtomicInteger()).incrementAndGet();
                }
                return null;
            }));
        }
        gate.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        log.info("result: hits={}; basis=smooth weighted round robin share == weight ratio 3:2:1", hits);
        assertEquals(60, hits.get("i1").get(), "权重 3 应分得 120*(3/6)=60 次");
        assertEquals(40, hits.get("i2").get(), "权重 2 应分得 120*(2/6)=40 次");
        assertEquals(20, hits.get("i3").get(), "权重 1 应分得 120*(1/6)=20 次");
    }

    @Test
    void inFlightSlotsNeverExceedLimitAndAreAlwaysReturned() throws Exception {
        var h = harness();
        h.registry.register(SVC, "i1", 1, 2); // 最多 2 个并发在途
        // 下游每次调用睡 30ms，制造真实的并发窗口
        var slow = new LatchDownstream(id -> h.instance(id)) {
            @Override
            public DownstreamResponse invoke(String instanceId, String payload, long timeoutMillis) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.invoke(instanceId, payload, timeoutMillis);
            }
        };
        var clock = new AtomicLong(1000);
        var registry = new InMemoryServiceRegistry(new HealthConfig(3, 1000, 100, 50), slow, clock::get);
        registry.register(SVC, "i1", 1, 2);
        var forwarder = new GatewayForwarder(registry, new WeightedRoundRobinLoadBalancer(), slow,
                new RetryPolicy(1, 0), new CircuitBreakerConfig(1000, 60_000, 1),
                clock::get, 5000, millis -> { });

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<ForwardOutcome>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int seq = i;
            futures.add(pool.submit(() -> {
                gate.await();
                return forwarder.forward(req("p" + seq));
            }));
        }
        gate.countDown();
        int succeeded = 0;
        int capacityRejected = 0;
        for (Future<ForwardOutcome> f : futures) {
            ForwardOutcome o = f.get();
            if (o.success()) {
                succeeded++;
            } else if (o.failureType() == FailureType.ALL_AT_CAPACITY) {
                capacityRejected++;
            }
        }
        pool.shutdown();
        int maxSeen = slow.maxInFlightOf("i1");
        int inFlightAfter = registry.getInstance(SVC, "i1").inFlight();
        log.info("input: 16 concurrent forwards, i1 maxInFlight=2, downstream latency 30ms; "
                        + "result: succeeded={}, capacityRejected={}, maxObservedInFlight={}, inFlightAfter={}; "
                        + "basis=slots never exceed limit, every acquired slot returned",
                succeeded, capacityRejected, maxSeen, inFlightAfter);
        assertTrue(maxSeen <= 2, "在途数绝不得超过上限 2，实际峰值=" + maxSeen);
        assertTrue(succeeded > 0 && capacityRejected > 0,
                "应既有成功放行也有满载快速失败，实际 succeeded=" + succeeded
                        + ", capacityRejected=" + capacityRejected);
        assertEquals(0, inFlightAfter, "所有请求结束后在途计数必须归零（无泄漏）");

        // 实例空下来后自动回到分发：后续请求照常成功
        ForwardOutcome after = forwarder.forward(req("after-drain"));
        log.info("input: forward after drain; result: success={}; basis=instance rejoins distribution",
                after.success());
        assertTrue(after.success());
    }

    @Test
    void slotsAreReturnedOnTimeoutAndDownstreamError() {
        var h = harness();
        h.registry.register(SVC, "i1", 1, 1);

        h.downstream.delegate.setMode("i1", SimulatedDownstream.Mode.TIMEOUT);
        ForwardOutcome timeout = h.forwarder.forward(req("t1"));
        log.info("input: i1 TIMEOUT; result: type={}, inFlight={}; basis=slot returned on timeout",
                timeout.failureType(), h.instance("i1").inFlight());
        assertFalse(timeout.success());
        assertEquals(0, h.instance("i1").inFlight(), "超时后名额必须归还");

        h.downstream.delegate.setMode("i1", SimulatedDownstream.Mode.ERROR_500);
        ForwardOutcome error = h.forwarder.forward(req("e1"));
        log.info("input: i1 ERROR_500; result: type={}, inFlight={}; basis=slot returned on 5xx",
                error.failureType(), h.instance("i1").inFlight());
        assertFalse(error.success());
        assertEquals(0, h.instance("i1").inFlight(), "下游报错后名额必须归还");

        h.downstream.delegate.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        ForwardOutcome ok = h.forwarder.forward(req("ok"));
        log.info("input: i1 NORMAL after failures; result: success={}; "
                + "basis=no leak => instance not stuck at capacity", ok.success());
        assertTrue(ok.success(), "名额无泄漏，实例不会被永久判成满载");
        assertEquals(0, h.instance("i1").inFlight());
    }

    @Test
    void fullInstanceIsSkippedAndRejoinsAfterDrain() throws Exception {
        var h = harness();
        h.registry.register(SVC, "i1", 1, 1); // i1 只能承载 1 个在途
        h.registry.register(SVC, "i2", 1, 0);

        // 占住 i1 的唯一名额
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        h.downstream.blockOn("i1", entered, release);
        // 等权重轮询第一选必中 i1（平滑加权轮询等权重退化为 i1,i2 交替）
        Future<ForwardOutcome> blocked = Executors.newSingleThreadExecutor()
                .submit(() -> h.forwarder.forward(req("blocked-on-i1")));
        assertTrue(entered.await(5, TimeUnit.SECONDS), "i1 的在途请求应已进入下游");
        assertEquals(1, h.instance("i1").inFlight());

        // i1 满载期间：所有新请求都应落到 i2，i1 被跳过且计入统计
        for (int i = 0; i < 6; i++) {
            ForwardOutcome o = h.forwarder.forward(req("during-full-" + i));
            assertTrue(o.success());
            assertEquals("i2", o.attempts().get(0).instanceId(),
                    "i1 满载时份额必须让给 i2");
        }
        long capacitySkips = h.forwarder.runtimeView(SVC).instances().stream()
                .filter(v -> v.instanceId().equals("i1")).findFirst().orElseThrow()
                .capacitySkipCount();
        log.info("input: 6 forwards while i1 at capacity 1/1; result: all served by i2, "
                + "i1 capacitySkips={}; basis=full instance skipped, share redistributed", capacitySkips);
        assertTrue(capacitySkips >= 6, "满载跳过必须计入只读视图统计");

        // 释放 i1：名额归还后自动回到分发
        h.downstream.unblock("i1");
        assertTrue(blocked.get(5, TimeUnit.SECONDS).success());
        assertEquals(0, h.instance("i1").inFlight(), "阻塞请求结束后名额必须归还");

        boolean i1ChosenAgain = false;
        for (int i = 0; i < 4 && !i1ChosenAgain; i++) {
            ForwardOutcome o = h.forwarder.forward(req("after-drain-" + i));
            i1ChosenAgain = o.attempts().get(0).instanceId().equals("i1");
        }
        log.info("input: forwards after i1 drained; result: i1ChosenAgain={}; "
                + "basis=drained instance rejoins weighted distribution", i1ChosenAgain);
        assertTrue(i1ChosenAgain, "i1 空下来后必须自动回到分发里");
    }
}
