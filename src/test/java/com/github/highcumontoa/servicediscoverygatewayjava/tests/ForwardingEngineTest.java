package com.github.highcumontoa.servicediscoverygatewayjava.tests;

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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 转发：失败类型区分、受限重试换实例、不可安全重试不重试、无实例快速失败。
 */
class ForwardingEngineTest {

    private static final Logger log = LoggerFactory.getLogger(ForwardingEngineTest.class);

    private ServiceInstance add(ServiceRegistry registry, InMemoryDownstreamTransport transport,
                                String id, Behavior behavior) {
        ServiceInstance meta = new ServiceInstance(id, "svc", "h", 1);
        registry.register(meta);
        transport.register(new SimulatedInstance(meta, ScriptedBehavior.always(behavior), 0L));
        return meta;
    }

    private ForwardingEngine engine(ServiceRegistry registry, InMemoryDownstreamTransport transport,
                                    int maxAttempts) {
        return new ForwardingEngine(registry, transport, new RoundRobinLoadBalancer(),
                new RetryPolicy(maxAttempts, 0), new IdempotencyDeduplicator(), 200);
    }

    @Test
    void retriesOnDifferentInstancesAndSucceeds() {
        ServiceRegistry registry = new ServiceRegistry(3, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();
        add(registry, transport, "i-bad1", Behavior.REFUSE);
        add(registry, transport, "i-bad2", Behavior.ERROR);
        add(registry, transport, "i-good", Behavior.NORMAL);
        ForwardingEngine engine = engine(registry, transport, 3);

        DispatchRequest req = new DispatchRequest("svc", "p", null, true);
        DispatchResult result = engine.forward(req);
        log.info("[T] 输入={} 选择结果 instance={} attempts={}；判定轨迹={}",
                req, result.instanceId(), result.attempts(), engine.lastFailures());

        assertEquals("i-good", result.instanceId());
        assertEquals(3, result.attempts(), "前两个实例失败后换到第三个成功");
        assertEquals(FailureType.CONNECTION_REFUSED, engine.lastFailures().get(0).failureType());
        assertEquals(FailureType.DOWNSTREAM_ERROR, engine.lastFailures().get(1).failureType());
    }

    @Test
    void nonRetryableRequestIsNotRetried() {
        ServiceRegistry registry = new ServiceRegistry(3, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();
        add(registry, transport, "i-bad", Behavior.REFUSE);
        add(registry, transport, "i-good", Behavior.NORMAL);
        ForwardingEngine engine = engine(registry, transport, 3);

        DispatchRequest req = new DispatchRequest("svc", "p", null, false);
        GatewayException e = assertThrows(GatewayException.class, () -> engine.forward(req));
        log.info("[T] 输入不可安全重试请求；失败类型={}；轨迹={}", e.getFailureType(), engine.lastFailures());
        assertEquals(FailureType.NON_RETRYABLE_REQUEST, e.getFailureType());
        assertEquals(1, engine.lastFailures().size(), "不可安全重试请求只能尝试一次");
        assertEquals(0, transport.instance("i-good").invokeCount(), "绝不能静默重试到其它实例");
    }

    @Test
    void retryExhaustedWhenAllDownstreamFail() {
        ServiceRegistry registry = new ServiceRegistry(3, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();
        add(registry, transport, "i1", Behavior.REFUSE);
        add(registry, transport, "i2", Behavior.REFUSE);
        ForwardingEngine engine = engine(registry, transport, 3);

        DispatchRequest req = new DispatchRequest("svc", "p", null, true);
        GatewayException e = assertThrows(GatewayException.class, () -> engine.forward(req));
        log.info("[T] 全部实例拒绝；最终失败类型={} 轨迹={}", e.getFailureType(), engine.lastFailures());
        assertEquals(FailureType.RETRY_EXHAUSTED, e.getFailureType());
        // 只有 2 个健康实例，不应重复重试同一实例放大压力
        assertEquals(2, engine.lastFailures().size());
        assertEquals(1, transport.instance("i1").invokeCount());
        assertEquals(1, transport.instance("i2").invokeCount());
    }

    @Test
    void noHealthyInstanceFailsFast() {
        ServiceRegistry registry = new ServiceRegistry(3, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();
        ForwardingEngine engine = engine(registry, transport, 3);

        GatewayException e = assertThrows(GatewayException.class,
                () -> engine.forward(new DispatchRequest("svc", "p", null, true)));
        log.info("[T] 空集群；失败类型={}", e.getFailureType());
        assertEquals(FailureType.NO_HEALTHY_INSTANCE, e.getFailureType());
    }

    @Test
    void timeoutMappedToDistinctFailureType() {
        ServiceRegistry registry = new ServiceRegistry(3, TimeUnit.SECONDS.toNanos(10), System::nanoTime);
        InMemoryDownstreamTransport transport = new InMemoryDownstreamTransport();
        add(registry, transport, "i-slow", Behavior.TIMEOUT);
        ForwardingEngine engine = new ForwardingEngine(registry, transport,
                new RoundRobinLoadBalancer(), new RetryPolicy(1, 0),
                new IdempotencyDeduplicator(), 50);

        GatewayException e = assertThrows(GatewayException.class,
                () -> engine.forward(new DispatchRequest("svc", "p", null, true)));
        log.info("[T] 超时实例；失败类型={} 轨迹={}", e.getFailureType(), engine.lastFailures());
        assertEquals(FailureType.TIMEOUT, e.getFailureType());
    }
}
