package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 实例在注册中心中的生命周期状态。
 */
public enum InstanceStatus {
    /** 健康：可参与流量分发 */
    HEALTHY,
    /** 不健康：已被健康探测从转发集合中摘除，但仍保留注册以便继续探测恢复 */
    UNHEALTHY,
    /** 已注销：不再属于任何服务 */
    DEREGISTERED
}
