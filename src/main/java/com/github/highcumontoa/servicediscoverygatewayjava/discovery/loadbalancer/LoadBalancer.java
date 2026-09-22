package com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;

import java.util.List;
import java.util.Set;

/**
 * 负载均衡器：在健康实例间按声明策略选择。
 * 健康集合变化时仅基于当前快照选择，保证流量平滑收敛。
 */
public interface LoadBalancer {

    /**
     * @param serviceName     服务名
     * @param healthy         当前健康实例快照
     * @param excludedInThisRequest 本次请求已尝试过、需排除的实例（重试时避开）
     * @return 选中实例；全部被排除时返回 null
     */
    ServiceInstance choose(String serviceName,
                           List<ServiceInstance> healthy,
                           Set<String> excludedInThisRequest);
}
