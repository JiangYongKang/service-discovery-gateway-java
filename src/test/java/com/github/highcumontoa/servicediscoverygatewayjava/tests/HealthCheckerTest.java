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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 健康探测：连续失败阈值摘除、隔离期跳过、恢复后回流量。
 */
class HealthCheckerTest {

    private static final Logger log = LoggerFactory.getLogger(HealthCheckerTest.class);

    @Test
    void evictAfterThresholdThenRecoverAfterQuarantine() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ServiceRegistry registry = new ServiceRegistry(5, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();

        // i-a：连续两次失败（REFUSE, REFUSE），之后恢复 NORMAL
        ServiceInstance a = new ServiceInstance("i-a", "svc", "h", 1);
        ScriptedBehavior script = ScriptedBehavior.of(List.of(Behavior.REFUSE, Behavior.REFUSE, Behavior.NORMAL));
        transport.register(new SimulatedInstance(a, script, 0L));
        registry.register(a);

        // 阈值=2，隔离期=80ms
        HealthChecker checker = new HealthChecker(
                registry, transport, 2, 60_000, 200, 80, scheduler);

        checker.probeOnce();
        log.info("[T] 第1轮后 i-a 失败计数={} 状态={} 依据=未达阈值",
                registry.consecutiveProbeFailures("svc", "i-a"), registry.statusOf("svc", "i-a"));
        assertEquals(1, registry.consecutiveProbeFailures("svc", "i-a"));
        assertEquals(InstanceStatus.HEALTHY, registry.statusOf("svc", "i-a"));

        checker.probeOnce();
        log.info("[T] 第2轮后 i-a 状态={} 健康快照={} 依据=连续2次失败被摘除",
                registry.statusOf("svc", "i-a"),
                registry.healthyInstances("svc").stream().map(ServiceInstance::id).toList());
        assertEquals(InstanceStatus.UNHEALTHY, registry.statusOf("svc", "i-a"));
        assertTrue(registry.healthyInstances("svc").isEmpty());

        // 隔离期内再探测：应跳过，不改变计数
        int probesBefore = transport.instance("i-a").probeCount();
        checker.probeOnce();
        assertEquals(probesBefore, transport.instance("i-a").probeCount(), "隔离期内不应探测");
        log.info("[T] 隔离期内探测次数保持={}，依据=摘除隔离期跳过", probesBefore);

        // 等隔离期过后再探测：脚本下一次是 NORMAL -> 恢复
        Thread.sleep(100);
        checker.probeOnce();
        log.info("[T] 隔离期过后探测，i-a 状态={} 健康快照={} 依据=探测成功恢复",
                registry.statusOf("svc", "i-a"),
                registry.healthyInstances("svc").stream().map(ServiceInstance::id).toList());
        assertEquals(InstanceStatus.HEALTHY, registry.statusOf("svc", "i-a"));
        assertEquals(1, registry.healthyInstances("svc").size());
        assertEquals(0, registry.consecutiveProbeFailures("svc", "i-a"));

        scheduler.shutdownNow();
    }
}
