package com.github.highcumontoa.servicediscoverygatewayjava.circuit;

/**
 * 熔断器状态：关闭（放行）、打开（快速失败）、半开（允许探测）。
 */
public enum CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
}
