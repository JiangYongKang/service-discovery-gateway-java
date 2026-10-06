package com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit;

/**
 * 熔断配置：失败阈值、熔断时长、半开探测成功所需连续成功次数、
 * 失败类型分布的统计窗口（状态视图只展示最近窗口内的失败构成，
 * 而不是自启动以来的只增累计）。
 */
public record CircuitBreakerConfig(int failureThreshold,
                                   long openMillis,
                                   int trialSuccessRequired,
                                   long failureWindowMillis) {

    /** 失败类型分布的默认统计窗口：60 秒。 */
    public static final long DEFAULT_FAILURE_WINDOW_MILLIS = 60_000L;

    /** 兼容构造：未指定统计窗口时使用默认 60 秒窗口。 */
    public CircuitBreakerConfig(int failureThreshold, long openMillis, int trialSuccessRequired) {
        this(failureThreshold, openMillis, trialSuccessRequired, DEFAULT_FAILURE_WINDOW_MILLIS);
    }
}
