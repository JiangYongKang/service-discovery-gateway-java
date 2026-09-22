package com.github.highcumontoa.servicediscoverygatewayjava.registry;

import com.github.highcumontoa.servicediscoverygatewayjava.circuit.CircuitBreaker;
import com.github.highcumontoa.servicediscoverygatewayjava.model.InstanceStatus;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 注册中心内部持有的实例条目：可变状态（状态、连续探测失败计数、摘除截止时间、熔断器）。
 * 实例元数据不可变；同一实例在注册中心中只有唯一条目，健康计数不会被重复计入。
 */
class RegisteredInstance {

    private final ServiceInstance instance;
    private final AtomicReference<InstanceStatus> status = new AtomicReference<>(InstanceStatus.HEALTHY);
    private final AtomicInteger consecutiveProbeFailures = new AtomicInteger();
    private volatile long evictUntilNanos;
    private final CircuitBreaker circuitBreaker;

    RegisteredInstance(ServiceInstance instance, CircuitBreaker circuitBreaker) {
        this.instance = instance;
        this.circuitBreaker = circuitBreaker;
    }

    ServiceInstance instance() {
        return instance;
    }

    InstanceStatus status() {
        return status.get();
    }

    boolean casStatus(InstanceStatus expected, InstanceStatus update) {
        return status.compareAndSet(expected, update);
    }

    void setStatus(InstanceStatus update) {
        status.set(update);
    }

    AtomicInteger probeFailures() {
        return consecutiveProbeFailures;
    }

    long getEvictUntilNanos() {
        return evictUntilNanos;
    }

    void setEvictUntilNanos(long value) {
        this.evictUntilNanos = value;
    }

    CircuitBreaker circuitBreaker() {
        return circuitBreaker;
    }
}
