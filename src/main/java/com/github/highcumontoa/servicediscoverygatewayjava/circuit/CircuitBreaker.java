package com.github.highcumontoa.servicediscoverygatewayjava.circuit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.LongSupplier;

/**
 * 单实例熔断器。
 *
 * 状态流转：
 * CLOSED --连续失败达到阈值--> OPEN（快速失败，不放大流量）
 * OPEN   --打开时长到期，且有请求到来--> HALF_OPEN（仅放行一个探测请求）
 * HALF_OPEN --探测成功--> CLOSED；--探测失败--> OPEN（重新计时，保持熔断）
 *
 * 时间由可注入的 {@code nanoTimeSupplier} 提供，测试中可驱动确定性的状态推进。
 */
public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    private final int failureThreshold;
    private final long openDurationNanos;
    private final LongSupplier nanoTimeSupplier;

    private CircuitState state = CircuitState.CLOSED;
    private int failureCount;
    private long openedAtNanos;

    public CircuitBreaker(int failureThreshold, long openDurationNanos) {
        this(failureThreshold, openDurationNanos, System::nanoTime);
    }

    public CircuitBreaker(int failureThreshold, long openDurationNanos, LongSupplier nanoTimeSupplier) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1");
        }
        this.failureThreshold = failureThreshold;
        this.openDurationNanos = openDurationNanos;
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    public synchronized CircuitState state() {
        // OPEN 到期是“惰性”转换：只有真正有请求尝试时才进入 HALF_OPEN
        return state;
    }

    /**
     * 当前是否允许一个请求通过：
     * CLOSED 直接放行；OPEN 到期后只放行第一个请求进入 HALF_OPEN；HALF_OPEN 期间其余请求拒绝。
     */
    public synchronized boolean allowRequest() {
        long now = nanoTimeSupplier.getAsLong();
        return switch (state) {
            case CLOSED -> true;
            case OPEN -> {
                if (now - openedAtNanos >= openDurationNanos) {
                    state = CircuitState.HALF_OPEN;
                    logStateTransition(CircuitState.OPEN, CircuitState.HALF_OPEN,
                            "打开时长到期，放行一个半开探测请求");
                    yield true;
                }
                yield false;
            }
            case HALF_OPEN -> false;
        };
    }

    public synchronized void recordSuccess() {
        if (state == CircuitState.HALF_OPEN) {
            logStateTransition(CircuitState.HALF_OPEN, CircuitState.CLOSED, "半开探测成功，恢复放行");
        }
        state = CircuitState.CLOSED;
        failureCount = 0;
        openedAtNanos = 0L;
    }

    public synchronized void recordFailure() {
        switch (state) {
            case HALF_OPEN -> {
                // 探测失败：回到 OPEN 并重新计时，保持熔断
                state = CircuitState.OPEN;
                openedAtNanos = nanoTimeSupplier.getAsLong();
                logStateTransition(CircuitState.HALF_OPEN, CircuitState.OPEN,
                        "半开探测失败，重新进入熔断");
            }
            case CLOSED -> {
                failureCount++;
                if (failureCount >= failureThreshold) {
                    state = CircuitState.OPEN;
                    openedAtNanos = nanoTimeSupplier.getAsLong();
                    logStateTransition(CircuitState.CLOSED, CircuitState.OPEN,
                            "连续失败 " + failureCount + " 次达到阈值 " + failureThreshold);
                }
            }
            case OPEN -> {
                // 快速失败路径理论上不会再记录失败（请求未放行），此处保持幂等
            }
        }
    }

    public synchronized void reset() {
        state = CircuitState.CLOSED;
        failureCount = 0;
        openedAtNanos = 0L;
    }

    private void logStateTransition(CircuitState from, CircuitState to, String reason) {
        log.info("[circuit] {} -> {}，依据：{}", from, to, reason);
    }
}
