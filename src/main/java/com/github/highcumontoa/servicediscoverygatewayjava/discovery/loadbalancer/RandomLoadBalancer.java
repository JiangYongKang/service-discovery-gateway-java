package com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * 随机负载均衡：在排除本次已尝试实例后的健康候选中均匀随机选择。
 * 集合变化时无需重置任何状态，天然平滑收敛。
 */
public class RandomLoadBalancer implements LoadBalancer {

    private static final Logger log = LoggerFactory.getLogger(RandomLoadBalancer.class);

    private final Random random;

    public RandomLoadBalancer() {
        this(new Random());
    }

    public RandomLoadBalancer(Random random) {
        this.random = random;
    }

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
        ServiceInstance chosen = candidates.get(random.nextInt(candidates.size()));
        log.info("lb input: service={}, policy=RANDOM, candidates={}, excluded={} -> choose={}",
                serviceName, candidates.size(), excluded, chosen.instanceId());
        return chosen;
    }
}
