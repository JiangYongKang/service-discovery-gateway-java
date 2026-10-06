package com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;

import java.util.Map;

/**
 * 单实例熔断器（CLOSED -> OPEN -> HALF_OPEN -> CLOSED/OPEN）。
 *
 * - 连续失败达阈值进入 OPEN，OPEN 期间调用一律快速失败，不放大下游压力；
 * - openMillis 后允许且仅允许一个探测请求进入 HALF_OPEN；
 * - 探测连续成功 trialSuccessRequired 次 -> CLOSED；任一失败 -> 重新 OPEN。
 */
public interface CircuitBreaker {

    String instanceId();

    CircuitState state();

    /** 是否允许当前请求通过（OPEN 时返回 false；到期时原子地放行唯一探测）。 */
    boolean allowRequest(long now);

    /**
     * 冷却（或失活的半开占坑）是否已经到点，到点后实例应当收到一次真实探测。
     * 纯查询，<strong>不改变任何状态</strong>（只读视图也可安全调用）。
     *
     * - OPEN 且 now-openedAt &gt;= openMillis -> true；
     * - HALF_OPEN 但占坑超过 probeStaleMillis（探测丢失）-> true，允许重新领取；
     * - 其它情况 -> false。
     */
    boolean isRecoveryProbeDue(long now);

    /**
     * 原子领取一次恢复探测：只有“冷却到期的 OPEN”或“探测失活的 HALF_OPEN”才能领取成功，
     * 成功后状态为 HALF_OPEN 且同一实例后续调用一律返回 false，直到探测结果落地
     * （recordSuccess/recordFailure）或占坑超时失活。
     *
     * 这样同一实例的探测窗口里<strong>恰好只有一个探测</strong>在飞，并发请求与
     * 后台周期探测都不会对它一拥而上；而不同实例各自独立领取、互不影响。
     */
    boolean tryAcquireRecoveryProbe(long now);

    /** 记录一次成功（HALF_OPEN 中可能触发恢复）。 */
    void recordSuccess(long now);

    /** 记录一次失败（HALF_OPEN 中立即回到 OPEN）。 */
    void recordFailure(long now);

    /** 记录一次带类型的失败（用于只读视图的失败类型分布统计）。 */
    default void recordFailure(long now, FailureType type) {
        recordFailure(now);
    }

    /** 是否正处于半开探测中（只读，不改变状态）。 */
    default boolean isProbing() {
        return state() == CircuitState.HALF_OPEN;
    }

    /**
     * 最近 {@code failureWindowMillis}（配置值）时间窗内的失败类型分布（只读快照）。
     * 纯查询：只对内部时间戳做过滤，不增删计数、不改变熔断状态。
     */
    default Map<FailureType, Integer> failureTypeDistribution() {
        return Map.of();
    }

    /** 带当前时间的只读分布查询：返回 [now-window, now] 窗口内的失败类型构成。 */
    default Map<FailureType, Integer> failureTypeDistribution(long now) {
        return failureTypeDistribution();
    }
}
