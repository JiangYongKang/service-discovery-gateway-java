package com.github.highcumontoa.servicediscoverygatewayjava.downstream;

/**
 * 模拟实例当前对“请求/探测”表现出的行为。
 */
public enum Behavior {
    /** 正常返回 200 */
    NORMAL,
    /** 模拟超时 */
    TIMEOUT,
    /** 模拟连接拒绝 */
    REFUSE,
    /** 返回下游错误（默认 500） */
    ERROR
}
