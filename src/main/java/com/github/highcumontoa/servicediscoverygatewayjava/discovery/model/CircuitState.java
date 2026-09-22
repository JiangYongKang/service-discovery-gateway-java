package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/** 熔断器状态：CLOSED 正常；OPEN 熔断快速失败；HALF_OPEN 半开探测中。 */
public enum CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
}
