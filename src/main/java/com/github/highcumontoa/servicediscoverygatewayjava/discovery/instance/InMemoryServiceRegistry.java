package com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.DownstreamHandler;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.HealthConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.InstanceStatus;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ServiceSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 进程内注册中心实现。
 *
 * 并发约束：
 * - 注册表为 ConcurrentHashMap；实例状态迁移在 ServiceInstance 的同步方法内完成；
 * - 探测按“服务名+实例ID”取细粒度锁，同一实例不可能被两个线程同时探测，
 *   因此同一探测结果不会被重复计数；
 * - healthyInstances 只返回状态为 HEALTHY 的实例，摘除/注销实例绝不参与转发；
 * - 摘除实例在冷却期（evictTimeMillis）内不探测；冷却结束后做一次复活探测，
 *   成功即清零并恢复 HEALTHY 重新参与流量，失败则留在 UNHEALTHY 等待下一轮。
 */
public class InMemoryServiceRegistry implements ServiceRegistry {

    private static final Logger log = LoggerFactory.getLogger(InMemoryServiceRegistry.class);

    private final HealthConfig config;
    private final DownstreamHandler healthProbe;
    private final LongSupplier clock;
    private final Map<String, Map<String, ServiceInstance>> services = new ConcurrentHashMap<>();

    public InMemoryServiceRegistry(HealthConfig config, DownstreamHandler healthProbe) {
        this(config, healthProbe, System::currentTimeMillis);
    }

    public InMemoryServiceRegistry(HealthConfig config,
                                   DownstreamHandler healthProbe,
                                   LongSupplier clock) {
        this.config = config;
        this.healthProbe = healthProbe;
        this.clock = clock;
    }

    @Override
    public boolean register(String serviceName, String instanceId) {
        return register(serviceName, instanceId, 1, ServiceInstance.UNLIMITED_CONCURRENCY);
    }

    @Override
    public boolean register(String serviceName, String instanceId, int weight, int maxConcurrency) {
        Map<String, ServiceInstance> instances =
                services.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());
        // putIfAbsent 保证并发注册同一实例只有一个胜出，绝不重复计入
        ServiceInstance created = instances.putIfAbsent(instanceId,
                new ServiceInstance(serviceName, instanceId, weight, maxConcurrency));
        if (created == null) {
            log.info("register input: service={}, instance={}, weight={}, maxConcurrency={} -> "
                            + "result=REGISTERED(initial HEALTHY)",
                    serviceName, instanceId, Math.max(1, weight),
                    maxConcurrency <= 0 ? "unlimited" : maxConcurrency);
            return true;
        }
        log.info("register input: service={}, instance={} -> result=IGNORED(already exists,status={})",
                serviceName, instanceId, created.status());
        return false;
    }

    @Override
    public RegistrationResult registerOrUpdate(String serviceName, String instanceId,
                                               int weight, int maxConcurrency) {
        Map<String, ServiceInstance> instances =
                services.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());
        ServiceInstance fresh = new ServiceInstance(serviceName, instanceId, weight, maxConcurrency);
        ServiceInstance existing = instances.putIfAbsent(instanceId, fresh);
        if (existing == null) {
            log.info("register-or-update input: service={}, instance={}, weight={}, maxConcurrency={} -> "
                            + "result=REGISTERED(initial HEALTHY)",
                    serviceName, instanceId, fresh.weight(),
                    fresh.maxConcurrency() == ServiceInstance.UNLIMITED_CONCURRENCY
                            ? "unlimited" : fresh.maxConcurrency());
            return new RegistrationResult(RegistrationOutcome.REGISTERED,
                    fresh.weight(), fresh.maxConcurrency());
        }
        // 已存在实例：灰度放量/缩量——只更新权重与并发上限，健康状态与失败计数保持不动。
        // 更新在实例的同步方法内完成，下一轮负载均衡读取到的就是新值。
        int[] effective = existing.updateLimits(weight, maxConcurrency);
        log.info("register-or-update input: service={}, instance={}, weight={}, maxConcurrency={} -> "
                        + "result=UPDATED(existing,status={}), effectiveWeight={}, effectiveMaxConcurrency={}, "
                        + "next selection round applies new values immediately",
                serviceName, instanceId, weight,
                maxConcurrency <= 0 ? "unlimited" : maxConcurrency,
                existing.status(), effective[0],
                effective[1] == ServiceInstance.UNLIMITED_CONCURRENCY ? "unlimited" : effective[1]);
        return new RegistrationResult(RegistrationOutcome.UPDATED, effective[0], effective[1]);
    }

    @Override
    public boolean deregister(String serviceName, String instanceId) {
        Map<String, ServiceInstance> instances = services.get(serviceName);
        if (instances == null) {
            log.info("deregister input: service={}, instance={} -> result=NOT_FOUND", serviceName, instanceId);
            return false;
        }
        ServiceInstance instance = instances.remove(instanceId);
        if (instance == null) {
            log.info("deregister input: service={}, instance={} -> result=NOT_FOUND", serviceName, instanceId);
            return false;
        }
        instance.deregister();
        log.info("deregister input: service={}, instance={} -> result=DEREGISTERED(removed from table)",
                serviceName, instanceId);
        return true;
    }

    @Override
    public void probeOnce(String serviceName) {
        Map<String, ServiceInstance> instances = services.get(serviceName);
        if (instances == null) {
            return;
        }
        long now = clock.getAsLong();
        // 复制快照后逐个探测；注销会从 map 移除，摘除/健康都保留；新注册会进入下一轮
        for (ServiceInstance instance : new ArrayList<>(instances.values())) {
            // 细粒度锁：同一实例的探测串行化，避免并发探测重复计数
            synchronized (probeLock(serviceName, instance.instanceId())) {
                // 拿到锁后重新确认：可能已在等待锁期间被注销
                if (instance.status() == InstanceStatus.DEREGISTERED
                        || instances.get(instance.instanceId()) != instance) {
                    log.debug("probe skip: instance={} reason=deregistered-while-waiting", instance.instanceId());
                    continue;
                }
                if (!instance.isHealthy()
                        && !instance.recoveryCooldownElapsed(now, config.evictTimeMillis())) {
                    long wait = config.evictTimeMillis() - (now - instance.evictedAtMillis());
                    log.debug("probe skip: instance={} reason=eviction-cooldown, remainMs={}",
                            instance.instanceId(), wait);
                    continue;
                }
                probeOne(instance, now);
            }
        }
    }

    private void probeOne(ServiceInstance instance, long now) {
        String id = instance.instanceId();
        try {
            healthProbe.invoke(id, "health-check", config.probeTimeoutMillis());
            instance.markProbeSuccess(now);
            log.info("probe result: instance={} -> UP; decision=HEALTHY(failures reset to 0, rejoins traffic)",
                    id);
        } catch (DownstreamException e) {
            boolean evicted = instance.markProbeFailure(now, config.failureThreshold());
            if (evicted) {
                log.warn("probe result: instance={} -> {}; decision=EVICT(reached {} consecutive failures, "
                                + "removed from traffic for {}ms)",
                        id, e.failureType(), config.failureThreshold(), config.evictTimeMillis());
            } else {
                log.info("probe result: instance={} -> {}; decision=KEEP_HEALTHY(consecutiveFailures={}/{})",
                        id, e.failureType(), instance.consecutiveFailures(), config.failureThreshold());
            }
        }
    }

    @Override
    public List<ServiceInstance> healthyInstances(String serviceName) {
        Map<String, ServiceInstance> instances = services.get(serviceName);
        List<ServiceInstance> healthy = new ArrayList<>();
        if (instances == null) {
            return healthy;
        }
        for (ServiceInstance instance : instances.values()) {
            if (instance.isHealthy()) {
                healthy.add(instance);
            }
        }
        return healthy;
    }

    @Override
    public ServiceInstance getInstance(String serviceName, String instanceId) {
        Map<String, ServiceInstance> instances = services.get(serviceName);
        return instances == null ? null : instances.get(instanceId);
    }

    @Override
    public ServiceSnapshot snapshot(String serviceName) {
        Map<String, ServiceInstance> instances = services.get(serviceName);
        List<String> healthyIds = new ArrayList<>();
        List<String> unhealthyIds = new ArrayList<>();
        if (instances != null) {
            for (ServiceInstance instance : instances.values()) {
                if (instance.isHealthy()) {
                    healthyIds.add(instance.instanceId());
                } else {
                    unhealthyIds.add(instance.instanceId());
                }
            }
        }
        return new ServiceSnapshot(serviceName, healthyIds, unhealthyIds);
    }

    @Override
    public HealthConfig config() {
        return config;
    }

    private final Map<String, Object> probeLocks = new ConcurrentHashMap<>();

    private Object probeLock(String serviceName, String instanceId) {
        return probeLocks.computeIfAbsent(serviceName + "/" + instanceId, k -> new Object());
    }
}
