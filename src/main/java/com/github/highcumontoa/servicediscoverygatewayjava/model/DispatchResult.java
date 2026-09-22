package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 一次转发的结果。
 *
 * @param duplicated 是否由幂等去重命中（复用了另一个并发请求的执行结果）
 */
public record DispatchResult(String instanceId, int statusCode, String body, int attempts, boolean duplicated) {
}
