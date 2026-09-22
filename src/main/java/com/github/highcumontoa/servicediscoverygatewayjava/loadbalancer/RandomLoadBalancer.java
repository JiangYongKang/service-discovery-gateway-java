package com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer;

import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机负载均衡器：在候选中均匀随机，排除本次请求已尝试过的实例。
 */
public class RandomLoadBalancer implements LoadBalancer {

    @Override
    public ServiceInstance choose(List<ServiceInstance> candidates, Set<String> excludeIds) {
        List<ServiceInstance> available = new ArrayList<>();
        for (ServiceInstance candidate : candidates) {
            if (excludeIds == null || !excludeIds.contains(candidate.id())) {
                available.add(candidate);
            }
        }
        if (available.isEmpty()) {
            return null;
        }
        return available.get(ThreadLocalRandom.current().nextInt(available.size()));
    }

    @Override
    public void onHealthySetChanged(List<ServiceInstance> healthy) {
        // 无内部游标，无需调整
    }
}
