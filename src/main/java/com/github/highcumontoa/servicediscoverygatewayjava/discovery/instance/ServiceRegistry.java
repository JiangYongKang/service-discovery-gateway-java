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
     * 带灰度权重与并发承载上限的注册。
     *
     * @param weight         分发权重（>=1；传 1 与老行为一致）
     * @param maxConcurrency 在途请求上限（0 表示不限，与老行为一致）
     */
    boolean register(String serviceName, String instanceId, int weight, int maxConcurrency);

    /**
     * 注册新实例，或对<strong>已存在</strong>实例在线更新权重与并发上限（灰度放量入口）。
     *
     * 与 {@link #register} 的区别：实例已存在时不再忽略，而是把 weight/maxConcurrency
     * 真正更新到运行实例上，下一轮负载均衡即按新值分摊。
     *
     * @return REGISTERED 表示新注册，UPDATED 表示更新了已存在实例；结果带归一化后的有效值
     */
    RegistrationResult registerOrUpdate(String serviceName, String instanceId,
                                        int weight, int maxConcurrency);

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

    /** registerOrUpdate 的结果：REGISTERED 新注册 / UPDATED 在线更新了已存在实例。 */
    enum RegistrationOutcome {
        REGISTERED, UPDATED
    }

    /**
     * 注册或更新结果。
     *
     * @param outcome         新注册还是更新已有实例
     * @param effectiveWeight 归一化后实际生效的权重（>=1）
     * @param effectiveMaxConcurrency 归一化后实际生效的并发上限（0=不限）
     */
    record RegistrationResult(RegistrationOutcome outcome,
                              int effectiveWeight,
                              int effectiveMaxConcurrency) {
    }
}
