package com.github.highcumontoa.servicediscoverygatewayjava.health;

import com.github.highcumontoa.servicediscoverygatewayjava.downstream.DownstreamTransport;
import com.github.highcumontoa.servicediscoverygatewayjava.model.InstanceStatus;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ProbeResult;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.registry.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 周期性健康探测器。
 *
 * 规则：
 * - 对 HEALTHY 实例探测：连续失败达 failureThreshold 次后摘为 UNHEALTHY，并记录摘除截止时间；
 * - 对 UNHEALTHY 实例：摘除隔离期内不探测（避免无效压力），隔离期过后继续探测，
 *   成功则恢复 HEALTHY 并清零计数/熔断，重新参与流量；
 * - 同一实例同一周期最多被探测一次（per-instance 锁），并发探测不会重复计入健康计数。
 */
public class HealthChecker {

    private static final Logger log = LoggerFactory.getLogger(HealthChecker.class);

    private final ServiceRegistry registry;
    private final DownstreamTransport transport;
    private final int failureThreshold;
    private final long intervalMillis;
    private final long timeoutMillis;
    private final long evictDurationMillis;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Object> probeLocks = new ConcurrentHashMap<>();
    /** 正在探测中的实例：保证同一时刻只有一个线程探测它，避免重复计数。 */
    private final Set<String> inProgress = ConcurrentHashMap.newKeySet();

    private ScheduledFuture<?> task;

    public HealthChecker(ServiceRegistry registry, DownstreamTransport transport,
                         int failureThreshold, long intervalMillis, long timeoutMillis,
                         long evictDurationMillis, ScheduledExecutorService scheduler) {
        this.registry = registry;
        this.transport = transport;
        this.failureThreshold = failureThreshold;
        this.intervalMillis = intervalMillis;
        this.timeoutMillis = timeoutMillis;
        this.evictDurationMillis = evictDurationMillis;
        this.scheduler = scheduler;
    }

    public void start() {
        if (task == null) {
            task = scheduler.scheduleWithFixedDelay(this::probeOnce,
                    intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
            log.info("[health] 探测器已启动 interval={}ms threshold={} timeout={}ms",
                    intervalMillis, failureThreshold, timeoutMillis);
        }
    }

    public void stop() {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
    }

    /** 立即执行一轮探测（测试用，同步完成）。基于当前实例快照，周期进行中注销的实例安全跳过。 */
    public void probeOnce() {
        // 以当前所有已注册服务的实例快照为输入
        List<String> serviceNames = registry.allServiceNames();
        for (String serviceName : serviceNames) {
            for (ServiceInstance instance : registry.allInstances(serviceName)) {
                probeOne(serviceName, instance);
            }
        }
    }

    private void probeOne(String serviceName, ServiceInstance instance) {
        String instanceId = instance.id();
        // 隔离期内的已摘除实例不探测，直到期限到达
        InstanceStatus current = registry.statusOf(serviceName, instanceId);
        if (current == null || current == InstanceStatus.DEREGISTERED) {
            return;
        }
        if (current == InstanceStatus.UNHEALTHY && !registry.quarantineElapsed(serviceName, instanceId)) {
            log.debug("[health] 实例 {}#{} 处于摘除隔离期，本轮跳过探测", serviceName, instanceId);
            return;
        }

        Object lock = probeLocks.computeIfAbsent(serviceName + "#" + instanceId, key -> new Object());
        synchronized (lock) {
            // 已有其它线程正在探测该实例：直接跳过，绝不重复计入健康状态
            String fullId = serviceName + "#" + instanceId;
            if (!inProgress.add(fullId)) {
                log.debug("[health] 实例 {} 正在被其它线程探测，本轮跳过", fullId);
                return;
            }
            try {
                // 双检：拿到锁后状态可能已被其它线程（周期线程/测试线程）改变
                current = registry.statusOf(serviceName, instanceId);
                if (current == null || current == InstanceStatus.DEREGISTERED) {
                    return;
                }
                if (current == InstanceStatus.UNHEALTHY && !registry.quarantineElapsed(serviceName, instanceId)) {
                    return;
                }

                ProbeResult result = transport.probe(instance, timeoutMillis);
                if (result.healthy()) {
                    registry.recordProbeSuccess(serviceName, instanceId);
                    boolean restored = registry.markHealthy(serviceName, instanceId);
                    log.info("[health] 实例 {}#{} 探测成功({})，判定={}",
                            serviceName, instanceId, result.reason(),
                            restored ? "恢复健康并回到流量集合" : "保持健康");
                } else {
                    int failures = registry.recordProbeFailure(serviceName, instanceId);
                    log.info("[health] 实例 {}#{} 探测失败({})，连续失败={}/{}",
                            serviceName, instanceId, result.reason(), failures, failureThreshold);
                    if (failures >= failureThreshold) {
                        // CAS 摘除：并发下只有一个线程真正执行，已摘除不会重复计时
                        registry.markUnhealthy(serviceName, instanceId,
                                TimeUnit.MILLISECONDS.toNanos(evictDurationMillis));
                    }
                }
            } finally {
                inProgress.remove(fullId);
            }
        }
    }
}
