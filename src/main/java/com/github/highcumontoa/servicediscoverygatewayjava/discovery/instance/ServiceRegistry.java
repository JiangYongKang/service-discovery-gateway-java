package com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.HealthConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ServiceSnapshot;

import java.util.List;

/**
 * 服务注册中心：注册 / 注销 / 健康探测 / 查询健康实例。
 */
public interface ServiceRegistry {

    /** 注册实例（幂等：已存在（健康或摘除中）时忽略并返回 false）。 */
    boolean register(String serviceName, String instanceId);

    /**
     * 带灰度与容量维度的注册。
     *
     * @param weight      流量分摊权重（>=1）
     * @param maxInFlight 并发在途上限（0 表示不限）
     */
    boolean register(String serviceName, String instanceId, int weight, int maxInFlight);

    /** 注销实例：之后永不再参与转发。返回是否真实注销。 */
    boolean deregister(String serviceName, String instanceId);

    /** 对单个服务执行一轮健康探测。 */
    void probeOnce(String serviceName);

    /** 获取某服务当前健康实例快照（摘除/注销的不得出现）。 */
    List<ServiceInstance> healthyInstances(String serviceName);

    /** 获取实例（无论状态），不存在返回 null。 */
    ServiceInstance getInstance(String serviceName, String instanceId);

    /** 解释性快照。 */
    ServiceSnapshot snapshot(String serviceName);

    /** 健康判定配置。 */
    HealthConfig config();
}
