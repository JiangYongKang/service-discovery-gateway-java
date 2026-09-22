package com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer;

import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询负载均衡器。
 * 健康集合收缩时游标按当前候选数取模，平滑收敛、不会选中已摘除实例。
 */
public class RoundRobinLoadBalancer implements LoadBalancer {

    private final AtomicInteger cursor = new AtomicInteger();

    @Override
    public ServiceInstance choose(List<ServiceInstance> candidates, Set<String> excludeIds) {
        int n = candidates.size();
        if (n == 0) {
            return null;
        }
        // 最多遍历一圈：候选都被排除（例如本次请求已试过）时返回 null
        int start = Math.floorMod(cursor.getAndIncrement(), n);
        for (int i = 0; i < n; i++) {
            ServiceInstance candidate = candidates.get((start + i) % n);
            if (excludeIds == null || !excludeIds.contains(candidate.id())) {
                return candidate;
            }
        }
        return null;
    }

    @Override
    public void onHealthySetChanged(List<ServiceInstance> healthy) {
        // 收缩后把游标压回有效范围，避免大游标长期偏移
        if (!healthy.isEmpty()) {
            cursor.set(Math.floorMod(cursor.get(), healthy.size()));
        }
    }
}
