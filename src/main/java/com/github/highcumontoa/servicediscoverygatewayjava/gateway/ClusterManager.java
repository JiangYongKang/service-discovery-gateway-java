package com.github.highcumontoa.servicediscoverygatewayjava.gateway;

import com.github.highcumontoa.servicediscoverygatewayjava.circuit.CircuitBreaker;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.Behavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.DownstreamTransport;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.ScriptedBehavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.SimulatedInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.model.InstanceStatus;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.registry.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 统一协调“注册中心 + 进程内模拟下游”：
 * 注册实例时同时建立模拟实例，注销时同步移除，并提供行为切换与状态视图。
 */
@Component
public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final ServiceRegistry registry;
    private final DownstreamTransport transport;

    public ClusterManager(ServiceRegistry registry, DownstreamTransport transport) {
        this.registry = registry;
        this.transport = transport;
    }

    /** 注册一个模拟实例，并返回是否为新注册。 */
    public boolean register(String serviceName, String instanceId, String host, int port,
                            ScriptedBehavior behavior) {
        ServiceInstance meta = new ServiceInstance(instanceId, serviceName, host, port);
        boolean added = registry.register(meta);
        SimulatedInstance existing = transport.instance(instanceId);
        if (existing == null) {
            transport.register(new SimulatedInstance(meta, behavior, 0L));
        } else {
            existing.setBehavior(behavior);
        }
        if (added) {
            log.info("[cluster] 实例 {}#{} 上线", serviceName, instanceId);
        }
        return added;
    }

    public boolean deregister(String serviceName, String instanceId) {
        return registry.deregister(serviceName, instanceId);
    }

    /** 切换实例行为：固定行为或脚本行为；latencyMillis 非空时同时调整模拟处理延迟。 */
    public boolean changeBehavior(String instanceId, String fixedBehavior, List<String> script,
                                  Long latencyMillis) {
        SimulatedInstance sim = transport.instance(instanceId);
        if (sim == null) {
            return false;
        }
        if (latencyMillis != null) {
            sim.setLatencyMillis(latencyMillis);
        }
        ScriptedBehavior target;
        if (script != null && !script.isEmpty()) {
            List<Behavior> behaviors = new ArrayList<>();
            for (String name : script) {
                behaviors.add(Behavior.valueOf(name.trim().toUpperCase()));
            }
            target = ScriptedBehavior.of(behaviors);
        } else {
            Behavior behavior = fixedBehavior == null || fixedBehavior.isBlank()
                    ? Behavior.NORMAL
                    : Behavior.valueOf(fixedBehavior.trim().toUpperCase());
            target = ScriptedBehavior.always(behavior);
        }
        sim.setBehavior(target);
        log.info("[cluster] 实例 {} 行为切换 script={} fixed={}", instanceId, script, fixedBehavior);
        return true;
    }

    /** 服务下全部实例的可观测状态视图。 */
    public Map<String, Object> status(String serviceName) {
        Map<String, Object> view = new LinkedHashMap<>();
        List<Map<String, Object>> items = new ArrayList<>();
        for (ServiceInstance instance : registry.allInstances(serviceName)) {
            InstanceStatus status = registry.statusOf(serviceName, instance.id());
            CircuitBreaker breaker = registry.circuitBreaker(serviceName, instance.id());
            SimulatedInstance sim = transport.instance(instance.id());
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("instanceId", instance.id());
            item.put("host", instance.host());
            item.put("port", instance.port());
            item.put("status", status);
            item.put("consecutiveProbeFailures",
                    registry.consecutiveProbeFailures(serviceName, instance.id()));
            item.put("circuitState", breaker == null ? null : breaker.state());
            item.put("invokeCount", sim == null ? null : sim.invokeCount());
            item.put("probeCount", sim == null ? null : sim.probeCount());
            items.add(item);
        }
        view.put("service", serviceName);
        view.put("healthy", registry.healthyInstances(serviceName)
                .stream().map(ServiceInstance::id).toList());
        view.put("instances", items);
        return view;
    }
}
