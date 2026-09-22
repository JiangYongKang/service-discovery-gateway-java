package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 下游返回的响应。
 */
public record DownstreamResponse(int statusCode, String body) {
}
