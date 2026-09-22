package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.InMemoryServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.HealthConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ServiceSnapshot;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 注册中心并发与健康判定测试：
 * - 并发注册同一实例只有一次生效，不重复计入；
 * - 连续失败达阈值即摘除，摘除实例不进健康集合；冷却后复活探测成功回归；
 * - 并发注册/注销/探测交替，也不会把已注销/摘除实例用于转发。
 */
class RegistryConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(RegistryConcurrencyTest.class);
    private static final String SVC = "order-service";

    private InMemoryServiceRegistry newRegistry(SimulatedDownstream downstream, AtomicLong clock) {
        HealthConfig config = new HealthConfig(3, 1000, 100, 50);
        return new InMemoryServiceRegistry(config, downstream, clock::get);
    }

    @Test
    void concurrentDuplicateRegistrationAppliesOnce() throws Exception {
        var downstream = new SimulatedDownstream(0);
        var registry = newRegistry(downstream, new AtomicLong(0));
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger trueCount = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                awaitQuietly(gate);
                if (registry.register(SVC, "shared-inst")) {
                    trueCount.incrementAndGet();
                }
            });
        }
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        log.info("input: {} threads register same instance; result: registerReturnedTrue={}, healthySize={}; "
                + "basis=putIfAbsent elects single winner",
                threads, trueCount.get(), registry.healthyInstances(SVC).size());
        assertEquals(1, trueCount.get(), "并发注册同一实例只能有一个返回 true");
        assertEquals(1, registry.healthyInstances(SVC).size(), "健康集合不得重复计入同一实例");
    }

    @Test
    void evictsAfterConsecutiveFailuresAndRecoversAfterCooldown() {
        var downstream = new SimulatedDownstream(0);
        var clock = new AtomicLong(1000);
        var registry = newRegistry(downstream, clock);
        registry.register(SVC, "i1");
        downstream.setMode("i1", SimulatedDownstream.Mode.REFUSE);

        registry.probeOnce(SVC);
        registry.probeOnce(SVC);
        ServiceSnapshot beforeThreshold = registry.snapshot(SVC);
        log.info("input: 2 failed probes (threshold=3); result: healthy={}, unhealthy={}; "
                + "basis=below threshold, stays in traffic",
                beforeThreshold.healthyIds(), beforeThreshold.unhealthyIds());
        assertEquals(1, beforeThreshold.healthyIds().size(), "未达阈值不得提前摘除");

        registry.probeOnce(SVC); // 第 3 次失败
        ServiceSnapshot evicted = registry.snapshot(SVC);
        log.info("input: 3rd failed probe; result: healthy={}, unhealthy={}; basis=threshold reached, EVICTED",
                evicted.healthyIds(), evicted.unhealthyIds());
        assertTrue(evicted.healthyIds().isEmpty(), "摘除实例绝不能出现在健康集合中");
        assertEquals(List.of("i1"), evicted.unhealthyIds());

        // 冷却期内探测被跳过
        clock.addAndGet(500);
        registry.probeOnce(SVC);
        log.info("input: probe within cooldown (500ms < 1000ms); result: still unhealthy={}; "
                + "basis=eviction cooldown protects probe traffic",
                registry.snapshot(SVC).unhealthyIds());
        assertFalse(registry.getInstance(SVC, "i1").isHealthy());

        // 冷却结束，下游恢复，复活探测成功
        clock.addAndGet(500);
        downstream.setMode("i1", SimulatedDownstream.Mode.NORMAL);
        registry.probeOnce(SVC);
        ServiceSnapshot recovered = registry.snapshot(SVC);
        log.info("input: cooldown elapsed + downstream back NORMAL; result: healthy={}; "
                + "basis=successful recovery probe resets failures and rejoins traffic",
                recovered.healthyIds());
        assertEquals(List.of("i1"), recovered.healthyIds(), "恢复后必须重新参与流量分发");
        assertEquals(0, registry.getInstance(SVC, "i1").consecutiveFailures());
    }

    @Test
    void concurrentRegisterDeregisterAndProbeNeverExposesStaleInstance() throws Exception {
        var downstream = new SimulatedDownstream(0);
        var clock = new AtomicLong(0);
        var registry = newRegistry(downstream, clock);
        downstream.setMode("victim", SimulatedDownstream.Mode.REFUSE);
        registry.register(SVC, "victim");
        for (int i = 1; i <= 5; i++) {
            registry.register(SVC, "stable-" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(6);
        CountDownLatch gate = new CountDownLatch(1);
        var observedHealthy = ConcurrentHashMap.newKeySet();

        // 不断注销/重注册 victim，同时高频探测
        for (int t = 0; t < 3; t++) {
            pool.submit(() -> {
                awaitQuietly(gate);
                for (int i = 0; i < 200; i++) {
                    registry.deregister(SVC, "victim");
                    registry.register(SVC, "victim");
                }
            });
        }
        pool.submit(() -> {
            awaitQuietly(gate);
            for (int i = 0; i < 400; i++) {
                clock.incrementAndGet();
                registry.probeOnce(SVC);
                for (ServiceInstance instance : registry.healthyInstances(SVC)) {
                    // 不变量：健康集合里返回的实例 isHealthy 必须恒为 true
                    if (!instance.isHealthy()) {
                        throw new AssertionError("non-healthy instance leaked into healthy set: " + instance);
                    }
                    observedHealthy.add(instance.instanceId());
                }
            }
        });
        gate.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        registry.deregister(SVC, "victim");
        ServiceSnapshot finalSnapshot = registry.snapshot(SVC);
        log.info("input: interleaved register/deregister/probe x400; result: finalHealthy={}, "
                + "everObserved={}; basis=deregistered instance can never be selected, "
                + "healthy-set invariant held throughout",
                finalSnapshot.healthyIds(), observedHealthy);
        assertFalse(finalSnapshot.healthyIds().contains("victim"));
        assertNull(registry.getInstance(SVC, "victim"), "注销后实例必须从注册表移除");
        for (int i = 1; i <= 5; i++) {
            assertTrue(finalSnapshot.healthyIds().contains("stable-" + i));
        }
    }

    private static void awaitQuietly(CountDownLatch gate) {
        try {
            gate.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
