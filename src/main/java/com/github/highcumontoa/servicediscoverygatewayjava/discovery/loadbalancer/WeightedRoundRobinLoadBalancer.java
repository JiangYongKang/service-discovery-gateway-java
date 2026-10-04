package com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 平滑加权轮询负载均衡（smooth weighted round robin，与 nginx 同算法）。
 *
 * - 每个实例注册时声明 weight（>=1，默认 1）；每轮选择时：
 *   current[i] += weight[i]，取 current 最大者，再令其 current -= totalWeight。
 *   长期看选中比例精确等于权重比，且分布平滑不扎堆；
 *   全部权重为 1 时退化为普通轮询，与既有默认行为一致。
 * - 容量治理：在途请求已达 maxConcurrency 的实例本轮被跳过（CAPACITY_FULL），
 *   它的权重份额按比例让给其余有余量的实例；名额归还后自动回到分发。
 * - 每服务的 current 状态表在 synchronized 块内推进，并发选择不会破坏权重不变量；
 *   不在当前候选中的残留状态会被清理，避免实例注销后状态泄漏。
 */
public class WeightedRoundRobinLoadBalancer implements LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(WeightedRoundRobinLoadBalancer.class);

    /** service -> (instanceId -> 平滑加权算法的 current 值) */
    private final Map<String, Map<String, AtomicInteger>> currentWeights = new ConcurrentHashMap<>();

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

        // 容量预筛：满载实例本轮让出份额（最终名额占用由调用方 tryAcquireInFlight 原子确认）
        List<ServiceInstance> available = new ArrayList<>();
        for (ServiceInstance instance : candidates) {
            if (instance.hasCapacity()) {
                available.add(instance);
            } else {
                log.info("lb skip: service={}, instance={} reason=CAPACITY_FULL "
                                + "(inFlight={}/max={}), its weight share goes to remaining instances",
                        serviceName, instance.instanceId(), instance.inFlight(), instance.maxConcurrency());
            }
        }
        if (available.isEmpty()) {
            log.info("lb input: service={}, candidates={} all CAPACITY_FULL, excluded={} -> choose=null",
                    serviceName, candidates.size(), excluded);
            return null;
        }

        Map<String, AtomicInteger> state =
                currentWeights.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());
        synchronized (state) {
            // 清理已不在候选中的残留状态（实例注销/摘除后权重状态不泄漏）
            state.keySet().removeIf(id -> candidates.stream()
                    .noneMatch(i -> i.instanceId().equals(id)));

            int totalWeight = available.stream().mapToInt(ServiceInstance::weight).sum();
            ServiceInstance best = null;
            int bestCurrent = Integer.MIN_VALUE;
            StringBuilder basis = new StringBuilder();
            for (ServiceInstance instance : available) {
                int current = state.computeIfAbsent(instance.instanceId(), k -> new AtomicInteger(0))
                        .addAndGet(instance.weight());
                basis.append(instance.instanceId())
                        .append("(w=").append(instance.weight())
                        .append(",cur=").append(current).append(") ");
                if (current > bestCurrent) {
                    bestCurrent = current;
                    best = instance;
                }
            }
            state.get(best.instanceId()).addAndGet(-totalWeight);
            log.info("lb input: service={}, policy=WEIGHTED_ROUND_ROBIN, available={}, excluded={} "
                            + "-> choose={} (basis: smooth WRR [{}], winner cur={} -= totalWeight={})",
                    serviceName, available.size(), excluded, best.instanceId(),
                    basis.toString().trim(), bestCurrent, totalWeight);
            return best;
        }
    }
}
