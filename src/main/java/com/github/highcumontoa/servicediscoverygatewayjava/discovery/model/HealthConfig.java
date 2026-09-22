package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/** 健康探测配置：连续失败阈值、摘除时限、探测周期与探测超时。 */
public record HealthConfig(int failureThreshold,
                          long evictTimeMillis,
                          long probeIntervalMillis,
                          long probeTimeoutMillis) {
}
