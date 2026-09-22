package com.github.highcumontoa.servicediscoverygatewayjava.tests;

import com.github.highcumontoa.servicediscoverygatewayjava.model.InstanceStatus;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发注册、注销与摘除：唯一条目、健康快照隔离、状态不重复计入。
 */
class ServiceRegistryTest {

    private static final Logger log = LoggerFactory.getLogger(ServiceRegistryTest.class);

    private ServiceRegistry newRegistry() {
        return new ServiceRegistry(3, TimeUnit.SECONDS.toNanos(2), System::nanoTime);
    }

    @Test
    void concurrentRegisterSameInstanceOnlyOnce() throws Exception {
        ServiceRegistry registry = newRegistry();
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger registeredOk = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    boolean ok = registry.register(new ServiceInstance("i1", "svc", "h", 1));
                    if (ok) {
                        registeredOk.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        pool.shutdown();

        log.info("[T] 输入：{} 个线程并发注册同一实例；选择结果：成功次数={}，健康快照={}；依据：putIfAbsent 唯一条目",
                threads, registeredOk.get(), registry.healthyInstances("svc"));
        assertEquals(1, registeredOk.get(), "同一实例只能被成功注册一次");
        assertEquals(1, registry.healthyInstances("svc").size());
        assertEquals(0, registry.consecutiveProbeFailures("svc", "i1"));
    }

    @Test
    void unhealthyNeverAppearsInHealthySnapshotAndRestore() {
        ServiceRegistry registry = newRegistry();
        registry.register(new ServiceInstance("i1", "svc", "h", 1));
        registry.register(new ServiceInstance("i2", "svc", "h", 2));

        boolean first = registry.markUnhealthy("svc", "i1", TimeUnit.SECONDS.toNanos(60));
        boolean second = registry.markUnhealthy("svc", "i1", TimeUnit.SECONDS.toNanos(60));
        log.info("[T] 输入：连续两次摘除 i1；结果 first={} second={}；依据：HEALTHY->UNHEALTHY CAS 唯一",
                first, second);
        assertTrue(first);
        assertFalse(second, "已摘除实例不得被重复摘除/重复计时");
        assertEquals(1, registry.healthyInstances("svc").size());
        assertEquals("i2", registry.healthyInstances("svc").get(0).id());
        assertEquals(InstanceStatus.UNHEALTHY, registry.statusOf("svc", "i1"));

        // 隔离期未到：不允许恢复探测路径
        assertFalse(registry.quarantineElapsed("svc", "i1"));

        registry.markHealthy("svc", "i1");
        assertEquals(2, registry.healthyInstances("svc").size(), "恢复后重新参与流量");
        log.info("[T] 恢复 i1 后健康快照={}，依据：UNHEALTHY->HEALTHY 并清零",
                registry.healthyInstances("svc").stream().map(ServiceInstance::id).toList());
    }

    @Test
    void deregisterRemovesFromSnapshotConcurrently() throws Exception {
        ServiceRegistry registry = newRegistry();
        registry.register(new ServiceInstance("i1", "svc", "h", 1));

        // 注销与读取健康快照并发：任何时刻快照要么含 i1 要么不含，绝不出现脏条目
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < 4; t++) {
            int seed = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int k = 0; k < 1000; k++) {
                        if (seed == 0 && k == 0) {
                            registry.deregister("svc", "i1");
                        }
                        var healthy = registry.healthyInstances("svc");
                        assertTrue(healthy.size() <= 1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(0, registry.healthyInstances("svc").size());
        log.info("[T] 并发注销/读取后健康快照为空，依据：移除即对快照不可见");
    }
}
