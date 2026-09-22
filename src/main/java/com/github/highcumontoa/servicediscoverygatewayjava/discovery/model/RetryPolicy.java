package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/** 重试策略：最大尝试次数与退避毫秒数（限制放大下游压力）。 */
public record RetryPolicy(int maxAttempts, long backoffMillis) {
}
