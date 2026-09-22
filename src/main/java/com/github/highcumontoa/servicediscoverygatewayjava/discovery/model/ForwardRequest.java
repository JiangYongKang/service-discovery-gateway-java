package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/** 待转发请求：服务名、幂等键、载荷与是否可安全重试。 */
public record ForwardRequest(String serviceName,
                             String idempotencyKey,
                             String payload,
                             boolean retryable) {
}
