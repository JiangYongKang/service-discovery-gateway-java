package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/** 下游调用结果：状态码与响应体。 */
public record DownstreamResponse(int statusCode, String body) {
}
