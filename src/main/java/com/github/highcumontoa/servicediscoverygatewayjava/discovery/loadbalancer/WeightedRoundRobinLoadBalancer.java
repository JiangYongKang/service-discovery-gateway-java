package com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 平滑加权轮询负载均衡（nginx 风格 smooth weighted round robin）。
 *
 * - 每个实例按注册权重分摊流量：每轮选择时 currentWeight += weight，
 *   取 currentWeight 最大者，并将其 currentWeight -= 总权重；
 *   长期看选中比例精确等于权重比，且分布平滑（不扎堆）；
 * - 等权重（默认 weight=1）时严格退化为普通轮询 i1,i2,i3,i1,...，与既有行为一致；
 * - 候选集合由调用方过滤（健康 ∩ 熔断放行 ∩ 未满载 ∩ 本次未试），
 *   满载/摘除实例不进入候选，其份额自然按剩余权重比例让给其他实例；
 * - 每服务一份状态，选择过程 synchronized，并发下分摊比例仍然准确。
 */
public class WeightedRoundRobinLoadBalancer implements LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(WeightedRoundRobinLoadBalancer.class);

    /** 每服务、每实例的当前动态权重。 */
    private final Map<String, Map<String, Integer>> currentWeights = new ConcurrentHashMap<>();

    @Override
    public ServiceInstance choose(String serviceName, List<ServiceInstance> healthy, Set<String> excluded) {
        List<ServiceInstance> candidates = healthy.stream()
                .filter(i -> !excluded.contains(i.instanceId()))
                .toList();
        if (candidates.isEmpty()) {
            log.info("lb input: service={}, healthy={}, excluded={} -> choose=null (no candidate)",
                    serviceName, healthy.size(), excluded);
            return null;
        }
        Map<String, Integer> weights =
                currentWeights.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());
        synchronized (weights) {
            // 清理已不在候选中的实例（摘除/注销/满载），避免陈旧状态扭曲分摊
            weights.keySet().retainAll(candidates.stream().map(ServiceInstance::instanceId).toList());
            int totalWeight = 0;
            ServiceInstance best = null;
            int bestWeight = Integer.MIN_VALUE;
            for (ServiceInstance candidate : candidates) {
                int updated = weights.getOrDefault(candidate.instanceId(), 0) + candidate.weight();
                weights.put(candidate.instanceId(), updated);
                totalWeight += candidate.weight();
                if (updated > bestWeight) {
                    bestWeight = updated;
                    best = candidate;
                }
            }
            weights.put(best.instanceId(), bestWeight - totalWeight);
            log.info("lb input: service={}, policy=WEIGHTED_ROUND_ROBIN, candidates={}, excluded={} "
                            + "-> choose={} (basis: smooth weighted round robin, share proportional to weight)",
                    serviceName, describe(candidates), excluded, best.instanceId());
            return best;
        }
    }

    private static List<String> describe(List<ServiceInstance> candidates) {
        List<String> described = new ArrayList<>();
        for (ServiceInstance c : candidates) {
            described.add(c.instanceId() + "(w=" + c.weight() + ")");
        }
        return described;
    }
}
