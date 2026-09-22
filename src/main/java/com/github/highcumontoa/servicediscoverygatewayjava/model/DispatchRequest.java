package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 一次转发请求的输入。
 *
 * @param safeRetry      是否可安全重试（GET/幂等请求应为 true）
 * @param idempotencyKey 幂等标识，可为 null；非空时相同标识的并发请求只执行一次
 */
public record DispatchRequest(String serviceName, String payload, String idempotencyKey, boolean safeRetry) {
}
