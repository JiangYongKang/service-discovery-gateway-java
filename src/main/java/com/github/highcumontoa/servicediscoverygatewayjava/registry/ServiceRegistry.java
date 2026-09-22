package com.github.highcumontoa.servicediscoverygatewayjava.registry;

import com.github.highcumontoa.servicediscoverygatewayjava.circuit.CircuitBreaker;
import com.github.highcumontoa.servicediscoverygatewayjava.model.InstanceStatus;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 进程内服务注册中心。
 *
 * 并发契约：
 * - register/deregister/markHealthy/markUnhealthy 均为原子操作；
 * - healthyInstances 返回不可变快照，摘除中的实例绝不会出现在健康快照中；
 * - 同一实例（serviceName + id）全局唯一条目，探测计数与熔断器各只有一份，不会重复计入。
 */
public class ServiceRegistry {

    private static final Logger log = LoggerFactory.getLogger(ServiceRegistry.class);

    private final int circuitFailureThreshold;
    private final long circuitOpenDurationNanos;
    private final LongSupplier nanoTimeSupplier;

    private final Map<String, Map<String, RegisteredInstance>> services = new ConcurrentHashMap<>();

    public ServiceRegistry(int circuitFailureThreshold, long circuitOpenDurationNanos,
                           LongSupplier nanoTimeSupplier) {
        this.circuitFailureThreshold = circuitFailureThreshold;
        this.circuitOpenDurationNanos = circuitOpenDurationNanos;
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    private Map<String, RegisteredInstance> instancesOf(String serviceName) {
        return services.computeIfAbsent(serviceName, key -> new ConcurrentHashMap<>());
    }

    /** 返回 true 表示新注册；false 表示实例已存在（幂等，不重复计数、不覆盖状态）。 */
    public boolean register(ServiceInstance instance) {
        Map<String, RegisteredInstance> instances = instancesOf(instance.serviceName());
        // putIfAbsent 保证同一实例只有唯一条目
        RegisteredInstance fresh = new RegisteredInstance(
                instance,
                new CircuitBreaker(circuitFailureThreshold, circuitOpenDurationNanos, nanoTimeSupplier));
        RegisteredInstance previous = instances.putIfAbsent(instance.id(), fresh);
        if (previous != null) {
            log.info("[registry] 注册忽略：实例 {}#{} 已存在，状态={}",
                    instance.serviceName(), instance.id(), previous.status());
            return false;
        }
        log.info("[registry] 注册实例 {}#{}", instance.serviceName(), instance.id());
        return true;
    }

    /** 返回 true 表示确实移除；false 表示本来就不存在。 */
    public boolean deregister(String serviceName, String instanceId) {
        Map<String, RegisteredInstance> instances = services.get(serviceName);
        if (instances == null) {
            return false;
        }
        RegisteredInstance removed = instances.remove(instanceId);
        if (removed == null) {
            return false;
        }
        removed.setStatus(InstanceStatus.DEREGISTERED);
        log.info("[registry] 注销实例 {}#{}，健康快照不再包含它", serviceName, instanceId);
        return true;
    }

    public List<ServiceInstance> healthyInstances(String serviceName) {
        Map<String, RegisteredInstance> instances = services.get(serviceName);
        if (instances == null || instances.isEmpty()) {
            return List.of();
        }
        return instances.values().stream()
                .filter(entry -> entry.status() == InstanceStatus.HEALTHY)
                .map(RegisteredInstance::instance)
                .sorted(Comparator.comparing(ServiceInstance::id))
                .toList();
    }

    public List<String> allServiceNames() {
        return List.copyOf(services.keySet());
    }

    public List<ServiceInstance> allInstances(String serviceName) {
        Map<String, RegisteredInstance> instances = services.get(serviceName);
        if (instances == null || instances.isEmpty()) {
            return List.of();
        }
        return instances.values().stream()
                .map(RegisteredInstance::instance)
                .sorted(Comparator.comparing(ServiceInstance::id))
                .toList();
    }

    public InstanceStatus statusOf(String serviceName, String instanceId) {
        RegisteredInstance entry = findEntry(serviceName, instanceId);
        return entry == null ? null : entry.status();
    }

    public Integer consecutiveProbeFailures(String serviceName, String instanceId) {
        RegisteredInstance entry = findEntry(serviceName, instanceId);
        return entry == null ? null : entry.probeFailures().get();
    }

    /**
     * 连续探测失败达到阈值时调用：仅 HEALTHY -> UNHEALTHY 的 CAS 获胜者真正摘除并记录截止时间，
     * 已摘除实例不会被重复摘除/重复计时。
     */
    public boolean markUnhealthy(String serviceName, String instanceId, long evictDurationNanos) {
        RegisteredInstance entry = findEntry(serviceName, instanceId);
        if (entry == null) {
            return false;
        }
        boolean swapped = entry.casStatus(InstanceStatus.HEALTHY, InstanceStatus.UNHEALTHY);
        if (swapped) {
            entry.setEvictUntilNanos(nanoTimeSupplier.getAsLong() + evictDurationNanos);
            log.info("[registry] 实例 {}#{} 连续探测失败达阈值，摘出转发集合至 {}ns 后",
                    serviceName, instanceId, entry.getEvictUntilNanos());
        }
        return swapped;
    }

    /** 探测恢复时调用：UNHEALTHY -> HEALTHY，清零计数并复位熔断器，重新参与流量。 */
    public boolean markHealthy(String serviceName, String instanceId) {
        RegisteredInstance entry = findEntry(serviceName, instanceId);
        if (entry == null) {
            return false;
        }
        boolean swapped = entry.casStatus(InstanceStatus.UNHEALTHY, InstanceStatus.HEALTHY);
        if (swapped) {
            entry.probeFailures().set(0);
            entry.setEvictUntilNanos(0L);
            entry.circuitBreaker().reset();
            log.info("[registry] 实例 {}#{} 探测恢复，重新参与流量分发", serviceName, instanceId);
        }
        return swapped;
    }

    /** 增加一次探测失败计数，返回更新后的值；实例不存在返回 null。 */
    public Integer recordProbeFailure(String serviceName, String instanceId) {
        RegisteredInstance entry = findEntry(serviceName, instanceId);
        return entry == null ? null : entry.probeFailures().incrementAndGet();
    }

    /** 探测成功：清零计数。 */
    public boolean recordProbeSuccess(String serviceName, String instanceId) {
        RegisteredInstance entry = findEntry(serviceName, instanceId);
        if (entry == null) {
            return false;
        }
        entry.probeFailures().set(0);
        return true;
    }

    /** 摘除隔离期限是否已到（未摘除实例恒为 true；不存在返回 false）。 */
    public boolean quarantineElapsed(String serviceName, String instanceId) {
        RegisteredInstance entry = findEntry(serviceName, instanceId);
        if (entry == null) {
            return false;
        }
        return entry.status() != InstanceStatus.UNHEALTHY
                || nanoTimeSupplier.getAsLong() >= entry.getEvictUntilNanos();
    }

    public CircuitBreaker circuitBreaker(String serviceName, String instanceId) {
        RegisteredInstance entry = findEntry(serviceName, instanceId);
        return entry == null ? null : entry.circuitBreaker();
    }

    private RegisteredInstance findEntry(String serviceName, String instanceId) {
        Map<String, RegisteredInstance> instances = services.get(serviceName);
        return instances == null ? null : instances.get(instanceId);
    }
}
