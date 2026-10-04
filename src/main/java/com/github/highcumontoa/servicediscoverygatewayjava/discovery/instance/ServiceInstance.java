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
 *
 * 灰度与容量维度：
 * - weight：流量分摊权重（默认 1，等权重时退化为普通轮询）；
 * - maxInFlight：并发在途上限（默认 0 表示不限）；在途计数用 CAS 获取/释放，
 *   名额必须成对归还，请求无论成功、超时还是下游报错都不能泄漏。
 */
public class ServiceInstance {

    /** 默认权重：等权重，保持既有轮询行为。 */
    public static final int DEFAULT_WEIGHT = 1;
    /** 默认并发上限：0 表示不限制，保持既有行为。 */
    public static final int UNLIMITED_IN_FLIGHT = 0;

    private final String serviceName;
    private final String instanceId;
    private final int weight;
    private final int maxInFlight;
    private final AtomicInteger inFlight;
    private volatile InstanceStatus status;
    private final AtomicInteger consecutiveFailures;
    private volatile long evictedAtMillis;
    private volatile long lastProbeResultAtMillis;

    public ServiceInstance(String serviceName, String instanceId) {
        this(serviceName, instanceId, DEFAULT_WEIGHT, UNLIMITED_IN_FLIGHT);
    }

    public ServiceInstance(String serviceName, String instanceId, int weight, int maxInFlight) {
        if (weight <= 0) {
            throw new IllegalArgumentException("weight must be positive: " + weight);
        }
        if (maxInFlight < 0) {
            throw new IllegalArgumentException("maxInFlight must be >= 0 (0 = unlimited): " + maxInFlight);
        }
        this.serviceName = serviceName;
        this.instanceId = instanceId;
        this.weight = weight;
        this.maxInFlight = maxInFlight;
        this.inFlight = new AtomicInteger(0);
        this.status = InstanceStatus.HEALTHY;
        this.consecutiveFailures = new AtomicInteger(0);
    }

    public String serviceName() {
        return serviceName;
    }

    public String instanceId() {
        return instanceId;
    }

    public int weight() {
        return weight;
    }

    /** 并发在途上限；0 表示不限制。 */
    public int maxInFlight() {
        return maxInFlight;
    }

    /** 当前在途请求数（观测用）。 */
    public int inFlight() {
        return inFlight.get();
    }

    /** 当前是否还有在途名额（不设上限时恒为 true）。 */
    public boolean hasCapacity() {
        return maxInFlight == UNLIMITED_IN_FLIGHT || inFlight.get() < maxInFlight;
    }

    /**
     * 原子获取一个在途名额：达到上限时返回 false，不占用名额。
     * CAS 循环保证并发下同一实例的在途计数准确，绝不会超限。
     */
    public boolean tryAcquireInFlight() {
        while (true) {
            int current = inFlight.get();
            if (maxInFlight != UNLIMITED_IN_FLIGHT && current >= maxInFlight) {
                return false;
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * 归还一个在途名额。必须与 tryAcquireInFlight 成功成对调用；
     * 计数防御性不为负（出现负值说明调用方配对有误，记日志场景下可发现）。
     */
    public void releaseInFlight() {
        inFlight.updateAndGet(current -> current > 0 ? current - 1 : 0);
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
        return instanceId + "[" + status + ",fails=" + consecutiveFailures.get()
                + ",w=" + weight + ",inFlight=" + inFlight.get()
                + (maxInFlight == UNLIMITED_IN_FLIGHT ? "" : "/" + maxInFlight) + "]";
    }
}
