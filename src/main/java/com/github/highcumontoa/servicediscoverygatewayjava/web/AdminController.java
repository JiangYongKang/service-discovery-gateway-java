package com.github.highcumontoa.servicediscoverygatewayjava.web;

import com.github.highcumontoa.servicediscoverygatewayjava.downstream.Behavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.ScriptedBehavior;
import com.github.highcumontoa.servicediscoverygatewayjava.gateway.ClusterManager;
import com.github.highcumontoa.servicediscoverygatewayjava.health.HealthChecker;
import com.github.highcumontoa.servicediscoverygatewayjava.web.dto.BehaviorRequestDto;
import com.github.highcumontoa.servicediscoverygatewayjava.web.dto.RegisterRequestDto;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 管理接口：注册/注销模拟实例、切换行为、触发探测、查看状态。
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final ClusterManager clusterManager;
    private final HealthChecker healthChecker;

    public AdminController(ClusterManager clusterManager, HealthChecker healthChecker) {
        this.clusterManager = clusterManager;
        this.healthChecker = healthChecker;
    }

    @PostMapping("/services/{serviceName}/instances")
    public Map<String, Object> register(@PathVariable String serviceName,
                                        @RequestBody RegisterRequestDto dto) {
        String instanceId = dto.getInstanceId() == null || dto.getInstanceId().isBlank()
                ? serviceName + "-" + System.identityHashCode(dto)
                : dto.getInstanceId();
        String behaviorName = dto.getBehavior() == null ? "NORMAL" : dto.getBehavior();
        boolean added = clusterManager.register(serviceName, instanceId,
                dto.getHost() == null ? "127.0.0.1" : dto.getHost(),
                dto.getPort() == null ? 0 : dto.getPort(),
                ScriptedBehavior.always(Behavior.valueOf(behaviorName.trim().toUpperCase())));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("registered", added);
        body.put("serviceName", serviceName);
        body.put("instanceId", instanceId);
        return body;
    }

    @DeleteMapping("/services/{serviceName}/instances/{instanceId}")
    public Map<String, Object> deregister(@PathVariable String serviceName,
                                          @PathVariable String instanceId) {
        boolean removed = clusterManager.deregister(serviceName, instanceId);
        return Map.of("deregistered", removed, "instanceId", instanceId);
    }

    @PutMapping("/services/{serviceName}/instances/{instanceId}/behavior")
    public Map<String, Object> behavior(@PathVariable String serviceName,
                                        @PathVariable String instanceId,
                                        @RequestBody BehaviorRequestDto dto) {
        boolean changed = clusterManager.changeBehavior(instanceId, dto.getBehavior(),
                dto.getScript(), dto.getLatencyMillis());
        return Map.of("changed", changed, "instanceId", instanceId);
    }

    @GetMapping("/services/{serviceName}")
    public Map<String, Object> status(@PathVariable String serviceName) {
        return clusterManager.status(serviceName);
    }

    @PostMapping("/health/probe")
    public Map<String, Object> probeOnce() {
        healthChecker.probeOnce();
        return Map.of("probe", "done");
    }
}
