package com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamResponse;

/**
 * 下游调用抽象：真实环境为 HTTP 客户端，本地由进程内模拟器实现。
 */
@FunctionalInterface
public interface DownstreamHandler {

    /**
     * 调用指定实例。
     *
     * @param instanceId    目标实例
     * @param payload       请求载荷
     * @param timeoutMillis 超时限制
     * @return 正常响应（含非 2xx，由调用方按状态码判定）
     * @throws com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamException
     *         超时 / 连接拒绝等传输类故障
     */
    DownstreamResponse invoke(String instanceId, String payload, long timeoutMillis);
}
