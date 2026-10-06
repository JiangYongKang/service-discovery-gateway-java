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
     * 冷却是否已到期、可以发起一次半开探测（只读，不改变状态）。
     * 仅 OPEN 且距打开超过 openMillis 时返回 true。
     */
    default boolean probeDue(long now) {
        return false;
    }

    /**
     * 原子地获取本实例的唯一探测权：仅当冷却到期（OPEN 且到时）时
     * 把状态翻为 HALF_OPEN 并返回 true；同一探测窗口内并发调用只有一个
     * 胜出，其余返回 false。调用方拿到 true 后必须真实发出一次下游探测，
     * 并以 recordSuccess/recordFailure 收尾。
     */
    default boolean tryAcquireProbe(long now) {
        return false;
    }

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

    /** 最近 failureWindowMillis 内的失败类型分布（只读快照，不改变状态）。 */
    default Map<FailureType, Integer> failureTypeDistribution(long now) {
        return Map.of();
    }
}
