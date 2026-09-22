package com.github.highcumontoa.servicediscoverygatewayjava.downstream;

import com.github.highcumontoa.servicediscoverygatewayjava.model.DownstreamResponse;
import com.github.highcumontoa.servicediscoverygatewayjava.model.FailureType;
import com.github.highcumontoa.servicediscoverygatewayjava.model.GatewayException;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ProbeResult;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内下游传输层：用模拟实例集群替代真实网络，
 * 但仍然抛出可区分 TIMEOUT / CONNECTION_REFUSED / DOWNSTREAM_ERROR 的异常。
 */
public class InMemoryDownstreamTransport implements DownstreamTransport {

    private static final Logger log = LoggerFactory.getLogger(InMemoryDownstreamTransport.class);

    private final Map<String, SimulatedInstance> instances = new ConcurrentHashMap<>();

    private static String key(ServiceInstance instance) {
        return instance.serviceName() + "#" + instance.id();
    }

    @Override
    public void register(SimulatedInstance instance) {
        instances.put(key(instance.meta()), instance);
        log.info("[transport] 注册模拟下游实例 {}", key(instance.meta()));
    }

    @Override
    public SimulatedInstance instance(String instanceId) {
        // 模拟场景下实例 id 全局唯一即可；保留按全限定键查询的能力在其它入口
        for (SimulatedInstance candidate : instances.values()) {
            if (candidate.meta().id().equals(instanceId)) {
                return candidate;
            }
        }
        return null;
    }

    @Override
    public DownstreamResponse invoke(ServiceInstance instance, String payload, long timeoutMillis) {
        SimulatedInstance sim = instances.get(key(instance));
        if (sim == null) {
            throw new GatewayException(FailureType.CONNECTION_REFUSED,
                    "实例不存在或不可达: " + key(instance));
        }
        Behavior behavior = sim.behavior().next();
        sim.incrementInvokeCount();
        log.info("[transport] 调用实例 {} 行为={} payload={} timeout={}ms",
                key(instance), behavior, payload, timeoutMillis);
        return switch (behavior) {
            case NORMAL -> {
                sleep(sim.latencyMillis());
                yield new DownstreamResponse(200, "OK from " + instance.id());
            }
            case TIMEOUT -> {
                // 模拟处理时间超过调用方给定的超时预算
                sleepQuietly(timeoutMillis + 100L);
                throw new GatewayException(FailureType.TIMEOUT,
                        "调用超时: " + key(instance) + " budget=" + timeoutMillis + "ms");
            }
            case REFUSE -> throw new GatewayException(FailureType.CONNECTION_REFUSED,
                    "连接被拒绝: " + key(instance));
            case ERROR -> new DownstreamResponse(500, "ERROR from " + instance.id());
        };
    }

    @Override
    public ProbeResult probe(ServiceInstance instance, long timeoutMillis) {
        SimulatedInstance sim = instances.get(key(instance));
        if (sim == null) {
            return ProbeResult.down("实例不存在");
        }
        Behavior behavior = sim.behavior().next();
        sim.incrementProbeCount();
        sim.enterProbe();
        try {
            log.debug("[transport] 探测实例 {} 行为={}", key(instance), behavior);
            return switch (behavior) {
                case NORMAL -> {
                    sleep(sim.latencyMillis());
                    yield ProbeResult.up("正常响应 200");
                }
                case TIMEOUT -> {
                    sleepQuietly(timeoutMillis + 50L);
                    yield ProbeResult.down("探测超时");
                }
                case REFUSE -> ProbeResult.down("连接被拒绝");
                case ERROR -> ProbeResult.down("下游返回错误 500");
            };
        } finally {
            sim.exitProbe();
        }
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        sleepQuietly(millis);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
