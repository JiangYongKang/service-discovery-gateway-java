package com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.InstanceStatus;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务实例：维护健康状态、连续失败计数与摘除时间戳。
 *
 * 并发约束：
 * - 状态迁移（HEALTHY/UNHEALTHY/DEREGISTERED）全部在 synchronized 方法内完成，
 *   杜绝并发探测把同一失败重复计数、或把摘除实例错误恢复；
 * - status / evictedAtMillis 使用 volatile 保证读线程可见；
 * - 已注销（DEREGISTERED）实例的探测结果一律忽略，永不复活。
 */
public class ServiceInstance {

    private final String serviceName;
    private final String instanceId;
    private volatile InstanceStatus status;
    private final AtomicInteger consecutiveFailures;
    private volatile long evictedAtMillis;
    private volatile long lastProbeResultAtMillis;

    public ServiceInstance(String serviceName, String instanceId) {
        this.serviceName = serviceName;
        this.instanceId = instanceId;
        this.status = InstanceStatus.HEALTHY;
        this.consecutiveFailures = new AtomicInteger(0);
    }

    public String serviceName() {
        return serviceName;
    }

    public String instanceId() {
        return instanceId;
    }

    public InstanceStatus status() {
        return status;
    }

    public int consecutiveFailures() {
        return consecutiveFailures.get();
    }

    public long evictedAtMillis() {
        return evictedAtMillis;
    }

    public long lastProbeResultAtMillis() {
        return lastProbeResultAtMillis;
    }

    /** 仅 HEALTHY 实例可参与转发。 */
    public boolean isHealthy() {
        return status == InstanceStatus.HEALTHY;
    }

    /**
     * 探测成功：失败计数清零；摘除中的实例恢复为 HEALTHY，重新参与流量。
     * 已注销实例忽略任何探测结果。
     */
    public synchronized void markProbeSuccess(long now) {
        if (status == InstanceStatus.DEREGISTERED) {
            return;
        }
        consecutiveFailures.set(0);
        status = InstanceStatus.HEALTHY;
        evictedAtMillis = 0L;
        lastProbeResultAtMillis = now;
    }

    /**
     * 探测失败：连续失败计数 +1；在 HEALTHY 状态下达到阈值时原子摘除。
     *
     * @return true 表示本次调用导致了 HEALTHY -> UNHEALTHY 摘除迁移
     */
    public synchronized boolean markProbeFailure(long now, int failureThreshold) {
        if (status == InstanceStatus.DEREGISTERED) {
            return false;
        }
        int fails = consecutiveFailures.incrementAndGet();
        lastProbeResultAtMillis = now;
        if (status == InstanceStatus.HEALTHY && fails >= failureThreshold) {
            status = InstanceStatus.UNHEALTHY;
            evictedAtMillis = now;
            return true;
        }
        return false;
    }

    /** 主动注销：终态，之后任何探测都不能使其复活。 */
    public synchronized void deregister() {
        if (status != InstanceStatus.DEREGISTERED) {
            status = InstanceStatus.DEREGISTERED;
            evictedAtMillis = 0L;
        }
    }

    /**
     * 摘除冷却是否已结束：只有冷却结束后才允许对摘除实例做复活探测，
     * 避免刚摘除就被探测流量反复打扰。
     */
    public boolean recoveryCooldownElapsed(long now, long evictTimeMillis) {
        return status == InstanceStatus.UNHEALTHY
                && now - evictedAtMillis >= evictTimeMillis;
    }

    @Override
    public String toString() {
        return instanceId + "[" + status + ",fails=" + consecutiveFailures.get() + "]";
    }
}
