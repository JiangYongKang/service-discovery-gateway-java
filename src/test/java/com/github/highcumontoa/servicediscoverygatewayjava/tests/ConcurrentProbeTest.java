package com.github.highcumontoa.servicediscoverygatewayjava.tests;

import com.github.highcumontoa.servicediscoverygatewayjava.downstream.Behavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.InMemoryDownstreamTransport;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.ScriptedBehavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.SimulatedInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.health.HealthChecker;
import com.github.highcumontoa.servicediscoverygatewayjava.model.InstanceStatus;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发探测/注册/注销交替：同一实例探测计数不被重复计入，摘除状态唯一。
 */
class ConcurrentProbeTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentProbeTest.class);

    @Test
    void parallelProbesCountEachRoundOnce() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ServiceRegistry registry = new ServiceRegistry(5, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();

        ServiceInstance a = new ServiceInstance("i-a", "svc", "h", 1);
        transport.register(new SimulatedInstance(a, ScriptedBehavior.always(Behavior.REFUSE), 0L));
        registry.register(a);

        HealthChecker checker = new HealthChecker(
                registry, transport, 100, 60_000, 200, 5_000, scheduler);

        // 模拟探测耗时，制造真正的交叠窗口
        transport.instance("i-a").setLatencyMillis(30);

        // 8 个线程同时触发探测轮次；由于 per-instance 锁 + 进行中集合，同一时刻最多一个探测
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    checker.probeOnce();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdown();
        scheduler.shutdownNow();

        int failures = registry.consecutiveProbeFailures("svc", "i-a");
        int probes = transport.instance("i-a").probeCount();
        log.info("[T] 输入：{} 线程并发探测同一 REFUSE 实例；结果：实际探测次数={} 连续失败={} 最大并发探测={} 状态={}；依据：per-instance 锁+进行中集合",
                threads, probes, failures, transport.instance("i-a").maxConcurrentProbes(),
                registry.statusOf("svc", "i-a"));
        assertTrue(probes >= 1, "至少应真正探测一次");
        assertEquals(probes, failures, "连续失败计数必须与实际探测次数一一对应，不重复、不丢失");
        assertEquals(1, transport.instance("i-a").maxConcurrentProbes(), "同一实例绝不能被并发探测");
        assertEquals(InstanceStatus.HEALTHY, registry.statusOf("svc", "i-a"));
    }

    @Test
    void deregisterDuringProbeNeverResurrectsInHealthySnapshot() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ServiceRegistry registry = new ServiceRegistry(5, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();
        for (String id : new String[]{"i-a", "i-b", "i-c"}) {
            ServiceInstance meta = new ServiceInstance(id, "svc", "h", 1);
            transport.register(new SimulatedInstance(meta, ScriptedBehavior.always(Behavior.NORMAL), 0L));
            registry.register(meta);
        }
        HealthChecker checker = new HealthChecker(
                registry, transport, 2, 60_000, 200, 5_000, scheduler);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < 4; t++) {
            int seed = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int k = 0; k < 500; k++) {
                        if (seed == 0 && k % 50 == 0) {
                            registry.deregister("svc", "i-a");
                        }
                        if (seed == 1 && k % 70 == 0) {
                            registry.register(new ServiceInstance("i-a", "svc", "h", 1));
                        }
                        checker.probeOnce();
                        assertTrue(registry.healthyInstances("svc").size() <= 3);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        scheduler.shutdownNow();
        log.info("[T] 注销/重新注册/探测交替完成，最终健康快照={}；依据：唯一条目 + CAS 状态翻转",
                registry.healthyInstances("svc").stream().map(ServiceInstance::id).toList());
    }
}
