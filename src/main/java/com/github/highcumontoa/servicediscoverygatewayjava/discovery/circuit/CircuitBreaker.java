package com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;

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

    /** 记录一次成功（HALF_OPEN 中可能触发恢复）。 */
    void recordSuccess(long now);

    /** 记录一次失败（HALF_OPEN 中立即回到 OPEN）。 */
    void recordFailure(long now);
}
