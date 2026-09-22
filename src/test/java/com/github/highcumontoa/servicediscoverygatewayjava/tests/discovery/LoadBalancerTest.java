package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.RandomLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.RoundRobinLoadBalancer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 分发策略与健康集合收敛：轮询顺序、重试排除、集合缩小时平滑收敛。 */
class LoadBalancerTest {

    private static final Logger log = LoggerFactory.getLogger(LoadBalancerTest.class);

    private List<ServiceInstance> instances(int n) {
        List<ServiceInstance> list = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            list.add(new ServiceInstance("svc", "i" + i));
        }
        return list;
    }

    @Test
    void roundRobinDistributesInOrder() {
        var lb = new RoundRobinLoadBalancer();
        var healthy = instances(3);
        List<String> picked = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            picked.add(lb.choose("svc", healthy, Set.of()).instanceId());
        }
        log.info("input: 3 healthy instances, 6 chooses; result={}; basis=round-robin cursor", picked);
        assertEquals(List.of("i1", "i2", "i3", "i1", "i2", "i3"), picked);
    }

    @Test
    void retriesExcludeAlreadyTriedInstances() {
        var lb = new RoundRobinLoadBalancer();
        var healthy = instances(3);
        Set<String> excluded = new HashSet<>();
        String first = lb.choose("svc", healthy, excluded).instanceId();
        excluded.add(first);
        String second = lb.choose("svc", healthy, excluded).instanceId();
        excluded.add(second);
        String third = lb.choose("svc", healthy, excluded).instanceId();
        log.info("input: exclude previous choices progressively; result={},{},{}; "
                + "basis=retry must not reuse a tried instance", first, second, third);
        assertEquals(3, Set.of(first, second, third).size(), "三次尝试必须落在不同实例");
        assertNull(lb.choose("svc", healthy, Set.of("i1", "i2", "i3")),
                "所有实例都试过且无其它候选时必须返回 null");
    }

    @Test
    void shrinksSmoothlyWhenHealthySetChanges() {
        var lb = new RoundRobinLoadBalancer();
        // 先把游标推进到任意值
        for (int i = 0; i < 7; i++) {
            lb.choose("svc", instances(3), Set.of());
        }
        var shrunk = instances(2);
        var picked = new HashSet<String>();
        for (int i = 0; i < 4; i++) {
            ServiceInstance chosen = lb.choose("svc", shrunk, Set.of());
            picked.add(chosen.instanceId());
        }
        log.info("input: healthy set shrank 3 -> 2 after cursor advanced; result=picked={}; "
                + "basis=cursor mod new size, never returns evicted instance", picked);
        assertEquals(Set.of("i1", "i2"), picked);
    }

    @Test
    void randomOnlyPicksHealthyNonExcluded() {
        var lb = new RandomLoadBalancer(new Random(42));
        var healthy = instances(4);
        Set<String> excluded = Set.of("i1", "i2", "i3");
        for (int i = 0; i < 5; i++) {
            String id = lb.choose("svc", healthy, excluded).instanceId();
            log.info("input: 4 healthy, excluded=i1,i2,i3; result={}; basis=only candidate left", id);
            assertEquals("i4", id);
        }
        assertTrue(lb.choose("svc", healthy, Set.of("i1", "i2", "i3", "i4")) == null);
    }
}
