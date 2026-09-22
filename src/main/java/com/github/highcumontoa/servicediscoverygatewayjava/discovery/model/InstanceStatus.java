package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/** 实例生命周期状态：HEALTHY 可参与流量；UNHEALTHY 已被摘除；DEREGISTERED 已注销。 */
public enum InstanceStatus {
    HEALTHY,
    UNHEALTHY,
    DEREGISTERED
}
