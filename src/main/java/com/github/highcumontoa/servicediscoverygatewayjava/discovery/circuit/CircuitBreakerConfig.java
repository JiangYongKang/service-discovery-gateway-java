package com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit;

/** 熔断配置：失败阈值、熔断时长、半开探测成功所需连续成功次数。 */
public record CircuitBreakerConfig(int failureThreshold,
                                   long openMillis,
                                   int trialSuccessRequired) {
}
