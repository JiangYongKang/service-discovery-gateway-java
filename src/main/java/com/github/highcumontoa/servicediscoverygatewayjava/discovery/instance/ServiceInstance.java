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

    /** 不限并发上限的标记值。 */
    public static final int UNLIMITED_CONCURRENCY = 0;

    private final String serviceName;
    private final String instanceId;
    /**
     * 灰度权重与并发上限支持灰度放量过程中在线调整：字段为 volatile，
     * 调整在 synchronized 方法内完成，负载均衡/容量预筛的读线程立即看到新值，
     * 下一轮选择即按新权重/新上限分摊，无需重新注册实例。
     */
    private volatile int weight;
    private volatile int maxConcurrency;
    private final AtomicInteger inFlight;
    private volatile InstanceStatus status;
    private final AtomicInteger consecutiveFailures;
    private volatile long evictedAtMillis;
    private volatile long lastProbeResultAtMillis;

    public ServiceInstance(String serviceName, String instanceId) {
        this(serviceName, instanceId, 1, UNLIMITED_CONCURRENCY);
    }

    public ServiceInstance(String serviceName, String instanceId, int weight, int maxConcurrency) {
        this.serviceName = serviceName;
        this.instanceId = instanceId;
        this.weight = Math.max(1, weight);
        this.maxConcurrency = Math.max(UNLIMITED_CONCURRENCY, maxConcurrency);
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

    /** 灰度权重：>=1，未指定时为 1（与老行为一致）。 */
    public int weight() {
        return weight;
    }

    /** 并发承载上限：0 表示不限（与老行为一致）。 */
    public int maxConcurrency() {
        return maxConcurrency;
    }

    /**
     * 在线调整灰度权重与并发上限（放量/缩量入口）。
     *
     * - 只改这两个运行参数，不动健康状态、失败计数与在途占用；
     * - 调整对下一轮负载均衡立即生效：平滑加权轮询直接读取最新 weight，
     *   容量预筛直接读取最新 maxConcurrency（正在途请求不受影响，结束后照常归还）；
     * - 入参非法（weight &lt; 1、maxConcurrency &lt; 0）时取与注册相同的兜底值。
     *
     * @return 归一化后的有效值
     */
    public synchronized int[] updateLimits(int newWeight, int newMaxConcurrency) {
        this.weight = Math.max(1, newWeight);
        this.maxConcurrency = Math.max(UNLIMITED_CONCURRENCY, newMaxConcurrency);
        return new int[]{this.weight, this.maxConcurrency};
    }

    /** 当前在途请求数。 */
    public int inFlight() {
        return inFlight.get();
    }

    /** 是否还有在途名额（不限上限时恒为 true）。 */
    public boolean hasCapacity() {
        return maxConcurrency <= UNLIMITED_CONCURRENCY || inFlight.get() < maxConcurrency;
    }

    /**
     * 原子占用一个在途名额；满载时返回 false，调用方本轮应跳过该实例。
     * CAS 循环保证并发下名额计数准确，绝不超发。
     */
    public boolean tryAcquireInFlight() {
        if (maxConcurrency <= UNLIMITED_CONCURRENCY) {
            inFlight.incrementAndGet();
            return true;
        }
        while (true) {
            int current = inFlight.get();
            if (current >= maxConcurrency) {
                return false;
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * 归还一个在途名额。无论请求成功、超时还是下游报错都必须调用，
     * 否则计数泄漏会把实例永久判成满载。
     */
    public void releaseInFlight() {
        inFlight.decrementAndGet();
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
