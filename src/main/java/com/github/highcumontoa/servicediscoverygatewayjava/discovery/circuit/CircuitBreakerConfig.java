package com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit;

/**
 * 熔断配置：失败阈值、熔断时长、半开探测成功所需连续成功次数。
 *
 * @param failureThreshold       连续真实调用失败多少次进入 OPEN
 * @param openMillis             OPEN 冷却时长，到期后允许一次半开探测
 * @param trialSuccessRequired   半开探测连续成功多少次恢复 CLOSED
 * @param probeStaleMillis       半开探测“失活”时限：HALF_OPEN 超过该时长仍无探测结果返回，
 *                               视为占坑请求丢失（线程被杀/调用方异常退出），允许后台探测
 *                               重新领取，保证实例不会永久卡在 HALF_OPEN
 * @param failureWindowMillis    只读视图中失败类型分布的统计窗口（最近一段时间口径，
 *                               而不是启动以来的历史累计）
 */
public record CircuitBreakerConfig(int failureThreshold,
                                   long openMillis,
                                   int trialSuccessRequired,
                                   long probeStaleMillis,
                                   long failureWindowMillis) {

    /** 兼容旧签名：探测失活时限取一次冷却时长，失败统计窗口默认 10 秒。 */
    public CircuitBreakerConfig(int failureThreshold, long openMillis, int trialSuccessRequired) {
        this(failureThreshold, openMillis, trialSuccessRequired, openMillis, 10_000L);
    }
}

