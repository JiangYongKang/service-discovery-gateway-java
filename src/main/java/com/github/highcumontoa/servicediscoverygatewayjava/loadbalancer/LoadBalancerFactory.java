package com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer;

public final class LoadBalancerFactory {

    private LoadBalancerFactory() {
    }

    public static LoadBalancer create(LoadBalancingPolicy policy) {
        return switch (policy) {
            case ROUND_ROBIN -> new RoundRobinLoadBalancer();
            case RANDOM -> new RandomLoadBalancer();
        };
    }
}
