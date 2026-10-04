package com.github.highcumontoa.servicediscoverygatewayjava.discovery.web;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.config.DiscoveryConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway.GatewayForwarder;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardOutcome;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ServiceSnapshot;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/** 本地演示与手工验证接口（服务统一为 demo-service）。 */
@RestController
@RequestMapping("/demo")
public class DemoController {

    private final ServiceRegistry registry;
    private final SimulatedDownstream downstream;
    private final GatewayForwarder forwarder;

    public DemoController(ServiceRegistry registry,
                          SimulatedDownstream downstream,
                          GatewayForwarder forwarder) {
        this.registry = registry;
        this.downstream = downstream;
        this.forwarder = forwarder;
    }

    /** 注册 count 个实例，实例ID为 inst-1..inst-count；可带权重与并发在途上限（0=不限）。 */
    @PostMapping("/register")
    public Map<String, Object> register(@RequestParam(defaultValue = "3") int count,
                                        @RequestParam(defaultValue = "1") int weight,
                                        @RequestParam(defaultValue = "0") int maxInFlight) {
        int created = 0;
        for (int i = 1; i <= count; i++) {
            String id = "inst-" + i;
            if (registry.register(DiscoveryConfig.DEMO_SERVICE, id, weight, maxInFlight)) {
                created++;
            }
            downstream.setMode(id, SimulatedDownstream.Mode.NORMAL);
        }
        return Map.of("requested", count, "newlyRegistered", created,
                "weight", weight, "maxInFlight", maxInFlight,
                "snapshot", registry.snapshot(DiscoveryConfig.DEMO_SERVICE));
    }

    /** 按实例注册（灰度场景：不同实例不同权重/容量）。 */
    @PostMapping("/instances/{id}/register")
    public Map<String, Object> registerOne(@PathVariable String id,
                                           @RequestParam(defaultValue = "1") int weight,
                                           @RequestParam(defaultValue = "0") int maxInFlight) {
        boolean created = registry.register(DiscoveryConfig.DEMO_SERVICE, id, weight, maxInFlight);
        downstream.setMode(id, SimulatedDownstream.Mode.NORMAL);
        return Map.of("instance", id, "newlyRegistered", created,
                "weight", weight, "maxInFlight", maxInFlight,
                "snapshot", registry.snapshot(DiscoveryConfig.DEMO_SERVICE));
    }

    /** 调整实例行为：NORMAL / REFUSE / TIMEOUT / ERROR_500。 */
    @PostMapping("/instances/{id}/mode")
    public Map<String, Object> setMode(@PathVariable String id,
                                       @RequestParam SimulatedDownstream.Mode mode) {
        downstream.setMode(id, mode);
        return Map.of("instance", id, "mode", mode);
    }

    /** 注销实例。 */
    @PostMapping("/instances/{id}/deregister")
    public Map<String, Object> deregister(@PathVariable String id) {
        boolean removed = registry.deregister(DiscoveryConfig.DEMO_SERVICE, id);
        return Map.of("instance", id, "deregistered", removed,
                "snapshot", registry.snapshot(DiscoveryConfig.DEMO_SERVICE));
    }

    /** 立即触发一轮健康探测（不必等待调度周期，便于手工复现摘除/恢复）。 */
    @PostMapping("/probe")
    public ServiceSnapshot probe() {
        registry.probeOnce(DiscoveryConfig.DEMO_SERVICE);
        return registry.snapshot(DiscoveryConfig.DEMO_SERVICE);
    }

    /** 通过网关转发请求，返回成功/失败、选择序列、失败类型与判定依据。 */
    @PostMapping("/forward")
    public ForwardOutcome forward(@RequestBody ForwardRequest request) {
        ForwardRequest effective = request.serviceName() == null || request.serviceName().isBlank()
                ? new ForwardRequest(DiscoveryConfig.DEMO_SERVICE, request.idempotencyKey(),
                request.payload(), request.retryable())
                : request;
        return forwarder.forward(effective);
    }

    /** 查看注册中心、熔断器与下游调用计数。 */
    @GetMapping("/state")
    public Map<String, Object> state() {
        Map<String, Object> state = new LinkedHashMap<>();
        ServiceSnapshot snapshot = registry.snapshot(DiscoveryConfig.DEMO_SERVICE);
        state.put("registry", snapshot);
        Map<String, Object> circuits = new LinkedHashMap<>();
        for (String id : snapshot.healthyIds()) {
            circuits.put(id, forwarder.circuitView(DiscoveryConfig.DEMO_SERVICE, id).state());
        }
        for (String id : snapshot.unhealthyIds()) {
            circuits.put(id, forwarder.circuitView(DiscoveryConfig.DEMO_SERVICE, id).state());
        }
        state.put("circuits", circuits);
        state.put("downstreamInvokeCounts", downstream.snapshotCounts());
        return state;
    }

    /**
     * 只读运行状态视图：每实例熔断状态、是否半开探测中、最近失败类型分布、
     * 被熔断/满载跳过累计次数、权重与在途容量。查看不改变任何运行状态。
     */
    @GetMapping("/runtime")
    public GatewayForwarder.RuntimeView runtime() {
        return forwarder.runtimeView(DiscoveryConfig.DEMO_SERVICE);
    }
}
