package com.github.highcumontoa.servicediscoverygatewayjava.downstream;

import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 进程内模拟的下游实例：可配置行为、处理延迟，并记录被调用次数（用于测试断言与日志）。
 */
public class SimulatedInstance {

    private final ServiceInstance meta;
    private volatile ScriptedBehavior behavior;
    private volatile long latencyMillis;
    private final AtomicInteger invokeCount = new AtomicInteger();
    private final AtomicInteger probeCount = new AtomicInteger();
    /** 当前正在执行的探测数：正常情况下绝不会大于 1。 */
    private final AtomicInteger concurrentProbes = new AtomicInteger();
    private final AtomicInteger maxConcurrentProbes = new AtomicInteger();

    public SimulatedInstance(ServiceInstance meta, ScriptedBehavior behavior, long latencyMillis) {
        this.meta = Objects.requireNonNull(meta);
        this.behavior = Objects.requireNonNull(behavior);
        this.latencyMillis = latencyMillis;
    }

    public ServiceInstance meta() {
        return meta;
    }

    public int invokeCount() {
        return invokeCount.get();
    }

    int incrementInvokeCount() {
        return invokeCount.incrementAndGet();
    }

    public int probeCount() {
        return probeCount.get();
    }

    int incrementProbeCount() {
        return probeCount.incrementAndGet();
    }

    void enterProbe() {
        int current = concurrentProbes.incrementAndGet();
        maxConcurrentProbes.accumulateAndGet(current, Math::max);
    }

    void exitProbe() {
        concurrentProbes.decrementAndGet();
    }

    public int maxConcurrentProbes() {
        return maxConcurrentProbes.get();
    }

    public ScriptedBehavior behavior() {
        return behavior;
    }

    public void setBehavior(ScriptedBehavior behavior) {
        this.behavior = Objects.requireNonNull(behavior);
    }

    public long latencyMillis() {
        return latencyMillis;
    }

    public void setLatencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
    }
}
