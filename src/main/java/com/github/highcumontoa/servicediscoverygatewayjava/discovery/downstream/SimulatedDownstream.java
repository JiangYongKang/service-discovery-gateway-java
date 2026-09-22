package com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamException;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamResponse;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 进程内下游集群模拟器。
 *
 * 每个实例可独立配置行为模式，使故障链路可解释、可复现：
 * - NORMAL    正常返回 200；
 * - REFUSE    模拟连接拒绝，抛 CONNECTION_REFUSED（传输故障，可安全重试）；
 * - TIMEOUT   模拟超时，等待可配置的模拟延迟后抛 TIMEOUT（传输故障，可安全重试）；
 * - ERROR_500 返回 500，计入真实执行（下游错误，默认不做自动重试）。
 *
 * 所有调用按实例与载荷计数：幂等去重测试据此断言“同一载荷只执行一次”。
 */
public class SimulatedDownstream implements DownstreamHandler {

    public enum Mode {NORMAL, REFUSE, TIMEOUT, ERROR_500}

    private static final Logger log = LoggerFactory.getLogger(SimulatedDownstream.class);

    private final Map<String, Mode> modes = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> invokeCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> payloadExecution = new ConcurrentHashMap<>();
    private volatile long timeoutSleepMillis;

    public SimulatedDownstream() {
        this(0L);
    }

    /** @param timeoutSleepMillis TIMEOUT 模式真实等待时长，测试可设 0 加速，演示可设大一些 */
    public SimulatedDownstream(long timeoutSleepMillis) {
        this.timeoutSleepMillis = timeoutSleepMillis;
    }

    public void setTimeoutSleepMillis(long millis) {
        this.timeoutSleepMillis = millis;
    }

    public void setMode(String instanceId, Mode mode) {
        modes.put(instanceId, mode);
        log.info("downstream mode set: instance={}, mode={}", instanceId, mode);
    }

    public Mode getMode(String instanceId) {
        return modes.getOrDefault(instanceId, Mode.NORMAL);
    }

    public int invokeCount(String instanceId) {
        return invokeCounts.getOrDefault(instanceId, new AtomicInteger()).get();
    }

    /** 某实例对某载荷的真实执行次数（NORMAL / ERROR_500 都算执行，REFUSE/TIMEOUT 不算）。 */
    public int payloadExecutionCount(String instanceId, String payload) {
        return payloadExecution.getOrDefault(executionKey(instanceId, payload), new AtomicInteger()).get();
    }

    public void resetCounts() {
        invokeCounts.clear();
        payloadExecution.clear();
    }

    @Override
    public DownstreamResponse invoke(String instanceId, String payload, long timeoutMillis) {
        Mode mode = getMode(instanceId);
        invokeCounts.computeIfAbsent(instanceId, k -> new AtomicInteger()).incrementAndGet();
        log.debug("invoke input: instance={}, mode={}, payload={}, timeoutMillis={}",
                instanceId, mode, payload, timeoutMillis);
        switch (mode) {
            case REFUSE -> {
                log.debug("invoke result: instance={} -> CONNECTION_REFUSED", instanceId);
                throw new DownstreamException(FailureType.CONNECTION_REFUSED,
                        "simulated connection refused: " + instanceId);
            }
            case TIMEOUT -> {
                sleepQuietly(timeoutSleepMillis);
                log.debug("invoke result: instance={} -> TIMEOUT (deadline={}ms)", instanceId, timeoutMillis);
                throw new DownstreamException(FailureType.TIMEOUT,
                        "simulated timeout after " + timeoutMillis + "ms: " + instanceId);
            }
            case ERROR_500 -> {
                payloadExecution.computeIfAbsent(executionKey(instanceId, payload),
                        k -> new AtomicInteger()).incrementAndGet();
                log.debug("invoke result: instance={} -> 500 (executed)", instanceId);
                return new DownstreamResponse(500, "simulated-error@" + instanceId);
            }
            default -> {
                payloadExecution.computeIfAbsent(executionKey(instanceId, payload),
                        k -> new AtomicInteger()).incrementAndGet();
                log.debug("invoke result: instance={} -> 200 (executed)", instanceId);
                return new DownstreamResponse(200, "ok@" + instanceId + ":" + payload);
            }
        }
    }

    protected void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Map<String, Integer> snapshotCounts() {
        Map<String, Integer> snapshot = new HashMap<>();
        invokeCounts.forEach((id, c) -> snapshot.put(id, c.get()));
        return snapshot;
    }

    private static String executionKey(String instanceId, String payload) {
        return instanceId + "#" + payload;
    }
}
