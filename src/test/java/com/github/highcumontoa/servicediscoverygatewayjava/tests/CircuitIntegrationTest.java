package com.github.highcumontoa.servicediscoverygatewayjava.tests;

import com.github.highcumontoa.servicediscoverygatewayjava.circuit.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.Behavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.InMemoryDownstreamTransport;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.ScriptedBehavior;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.SimulatedInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.gateway.ForwardingEngine;
import com.github.highcumontoa.servicediscoverygatewayjava.idempotency.IdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer.RoundRobinLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DispatchRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DispatchResult;
import com.github.highcumontoa.servicediscoverygatewayjava.model.FailureType;
import com.github.highcumontoa.servicediscoverygatewayjava.model.GatewayException;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.registry.ServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.retry.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 熔断与转发联动：单实例连续失败 -> 熔断快速失败 -> 半开探测成功恢复；探测失败保持熔断。
 */
class CircuitIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitIntegrationTest.class);

    @Test
    void breakerOpensFastFailsThenHalfOpenProbeRecovers() {
        AtomicLong clock = new AtomicLong();
        ServiceRegistry registry = new ServiceRegistry(3, 1000L, clock::get);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();

        // 前三次 REFUSE 触发熔断，第四次 NORMAL 作为半开探测
        ServiceInstance meta = new ServiceInstance("i1", "svc", "h", 1);
        SimulatedInstance sim = new SimulatedInstance(meta,
                ScriptedBehavior.of(List.of(Behavior.REFUSE, Behavior.REFUSE, Behavior.REFUSE, Behavior.NORMAL)),
                0L);
        transport.register(sim);
        registry.register(meta);

        ForwardingEngine engine = new ForwardingEngine(registry, transport,
                new RoundRobinLoadBalancer(), new RetryPolicy(1, 0),
                new IdempotencyDeduplicator(), 200);
        DispatchRequest req = new DispatchRequest("svc", "p", null, true);

        for (int i = 0; i < 3; i++) {
            GatewayException e = assertThrows(GatewayException.class, () -> engine.forward(req));
            assertEquals(FailureType.CONNECTION_REFUSED, e.getFailureType());
        }
        assertEquals(CircuitState.OPEN, registry.circuitBreaker("svc", "i1").state());
        log.info("[T] 3 次失败后熔断 OPEN，下游被调用次数={}（熔断前）", sim.invokeCount());

        // 熔断打开：快速失败，不调用下游
        int callsBefore = sim.invokeCount();
        GatewayException open = assertThrows(GatewayException.class, () -> engine.forward(req));
        assertEquals(FailureType.CIRCUIT_OPEN, open.getFailureType());
        assertEquals(callsBefore, sim.invokeCount(), "熔断期间不得继续放大下游压力");
        log.info("[T] 熔断期请求失败类型=CIRCUIT_OPEN，调用次数保持={} 依据=未调用下游", sim.invokeCount());

        // 打开到期：半开探测（脚本下一个是 NORMAL）成功 -> CLOSED
        clock.addAndGet(1001L);
        DispatchResult result = engine.forward(req);
        assertEquals(CircuitState.CLOSED, registry.circuitBreaker("svc", "i1").state());
        assertEquals("i1", result.instanceId());
        log.info("[T] 到期后半开探测成功，状态=CLOSED，选择结果 instance={}", result.instanceId());
    }

    @Test
    void halfOpenProbeFailureKeepsOpen() {
        AtomicLong clock = new AtomicLong();
        ServiceRegistry registry = new ServiceRegistry(2, 1000L, clock::get);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();
        ServiceInstance meta = new ServiceInstance("i1", "svc", "h", 1);
        SimulatedInstance sim = new SimulatedInstance(meta,
                ScriptedBehavior.of(List.of(Behavior.REFUSE, Behavior.REFUSE, Behavior.REFUSE)), 0L);
        transport.register(sim);
        registry.register(meta);

        ForwardingEngine engine = new ForwardingEngine(registry, transport,
                new RoundRobinLoadBalancer(), new RetryPolicy(1, 0),
                new IdempotencyDeduplicator(), 200);
        DispatchRequest req = new DispatchRequest("svc", "p", null, true);

        assertThrows(GatewayException.class, () -> engine.forward(req));
        assertThrows(GatewayException.class, () -> engine.forward(req));
        assertEquals(CircuitState.OPEN, registry.circuitBreaker("svc", "i1").state());

        clock.addAndGet(1001L);
        // 半开放行的探测仍失败 -> 回到 OPEN
        GatewayException e = assertThrows(GatewayException.class, () -> engine.forward(req));
        assertEquals(FailureType.CONNECTION_REFUSED, e.getFailureType());
        assertEquals(CircuitState.OPEN, registry.circuitBreaker("svc", "i1").state());
        log.info("[T] 半开探测失败，熔断保持 OPEN（重新计时）");
    }
}
