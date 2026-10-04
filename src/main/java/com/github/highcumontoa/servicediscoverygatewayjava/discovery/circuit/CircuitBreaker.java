package com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;

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
     * 尝试获取半开探测权：仅当 OPEN 且冷却到期时，原子地迁移到 HALF_OPEN 并返回 true。
     * 同一探测窗口内并发调用只有一个能成功，其余返回 false——探测请求不会一拥而上。
     * 纯查询场景请使用 {@link #state()}，本方法会改变状态，只应由真正要发探测的请求调用。
     */
    boolean tryAcquireProbe(long now);

    /**
     * 放弃已获取的探测权（例如探测实例恰好满载无法执行）：回到 OPEN 并重新计时，
     * 等待下一轮探测窗口。仅在 {@link #tryAcquireProbe} 成功后、探测未真实发出时调用。
     */
    void releaseProbe(long now);

    /** 记录一次成功（HALF_OPEN 中可能触发恢复）。 */
    void recordSuccess(long now);

    /** 记录一次失败（HALF_OPEN 中立即回到 OPEN）。 */
    void recordFailure(long now);

    /** 记录一次失败并携带失败类型（用于运行视图的最近失败类型分布）。 */
    default void recordFailure(long now, FailureType failureType) {
        recordFailure(now);
    }
}

