package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 一个下游服务实例的不可变元数据。
 */
public record ServiceInstance(String id, String serviceName, String host, int port) {
}
