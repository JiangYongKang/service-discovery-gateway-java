package com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer;

import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;

import java.util.List;

/**
 * 负载均衡器：在健康实例快照上做选择。
 */
public interface LoadBalancer {

    /**
     * 从 candidates 中选择一个不在 excludeIds 中的实例；无可用时返回 null。
     */
    ServiceInstance choose(List<ServiceInstance> candidates, java.util.Set<String> excludeIds);

    /** 健康集合变化时调用，使选择状态平滑收敛（如轮询游标重置到有效范围）。 */
    void onHealthySetChanged(List<ServiceInstance> healthy);
}
