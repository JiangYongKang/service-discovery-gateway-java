package com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询负载均衡。
 *
 * - 每服务一个游标，CAS 自增，对候选数量取模；
 * - 选择仅依赖调用方传入的“当前健康快照”，实例摘除导致集合缩小时取模自然收敛，
 *   不会选到已摘除实例，也不会出现流量长期倾斜；
 * - 重试时通过 excluded 排除本次已尝试实例，并继续推进游标，避免把重试压力
 *   重新打到故障实例上。
 */
public class RoundRobinLoadBalancer implements LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(RoundRobinLoadBalancer.class);

    private final Map<String, AtomicInteger> cursors = new ConcurrentHashMap<>();

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
        AtomicInteger cursor = cursors.computeIfAbsent(serviceName, k -> new AtomicInteger(0));
        int index = Math.floorMod(cursor.getAndIncrement(), candidates.size());
        ServiceInstance chosen = candidates.get(index);
        log.info("lb input: service={}, policy=ROUND_ROBIN, candidates={}, excluded={} "
                        + "-> choose={} (basis: cursor mod candidates)",
                serviceName, candidates.size(), excluded, chosen.instanceId());
        return chosen;
    }
}
