package com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.Map;

/**
 * 默认单实例熔断器，所有状态迁移在 synchronized 方法内完成。
 *
 * 状态机：
 * - CLOSED：正常放行；连续失败计数达到 failureThreshold 时迁到 OPEN 并记录打开时刻；
 * - OPEN：拒绝一切请求（快速失败，不向下游放大流量）；距打开超过 openMillis 后，
 *   allowRequest 原子地把状态翻到 HALF_OPEN，且同一时刻只放行一个探测请求；
 * - HALF_OPEN：探测请求成功 trialSuccessRequired 次（模拟器配置为 1）-> CLOSED 并清零；
 *   任一失败 -> 立即回到 OPEN，重新计时，等待下一轮半开探测。
 *
 * 观测：保留最近 {@value #RECENT_FAILURE_WINDOW} 次真实失败的类型分布，
 * 供只读运行视图查询；读取分布不改变任何状态。
 */
public class DefaultCircuitBreaker implements CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(DefaultCircuitBreaker.class);

    /** 最近失败类型分布的滑动窗口大小（按次数）。 */
    private static final int RECENT_FAILURE_WINDOW = 32;

    private final String instanceId;
    private final CircuitBreakerConfig config;

    private CircuitState state = CircuitState.CLOSED;
    private int consecutiveFailures = 0;
    private int halfOpenSuccesses = 0;
    private long openedAtMillis = 0L;
    private final FailureType[] recentFailures = new FailureType[RECENT_FAILURE_WINDOW];
    private int recentFailureCursor = 0;
    private int recentFailureSize = 0;

    public DefaultCircuitBreaker(String instanceId, CircuitBreakerConfig config) {
        this.instanceId = instanceId;
        this.config = config;
    }

    @Override
    public String instanceId() {
        return instanceId;
    }

    @Override
    public synchronized CircuitState state() {
        return state;
    }

    @Override
    public synchronized boolean allowRequest(long now) {
        switch (state) {
            case CLOSED -> {
                return true;
            }
            case OPEN -> {
                return tryAcquireProbe(now);
            }
            case HALF_OPEN -> {
                // 探测请求尚未返回，其它请求一律快速失败
                log.debug("circuit[{}] reject: state=HALF_OPEN, trial in progress", instanceId);
                return false;
            }
            default -> {
                return false;
            }
        }
    }

    @Override
    public synchronized boolean tryAcquireProbe(long now) {
        if (state != CircuitState.OPEN) {
            return false;
        }
        long elapsed = now - openedAtMillis;
        if (elapsed < config.openMillis()) {
            log.debug("circuit[{}] probe not yet due: state=OPEN, remaining={}ms",
                    instanceId, config.openMillis() - elapsed);
            return false;
        }
        // 原子放行唯一探测请求：状态立刻变 HALF_OPEN，后续请求继续被拒
        state = CircuitState.HALF_OPEN;
        halfOpenSuccesses = 0;
        log.info("circuit[{}] OPEN -> HALF_OPEN: elapsed={}ms >= openMillis={}ms, "
                + "exactly ONE trial request allowed", instanceId, elapsed, config.openMillis());
        return true;
    }

    @Override
    public synchronized void releaseProbe(long now) {
        if (state != CircuitState.HALF_OPEN) {
            return;
        }
        state = CircuitState.OPEN;
        halfOpenSuccesses = 0;
        openedAtMillis = now;
        log.info("circuit[{}] HALF_OPEN -> OPEN: probe released before execution "
                + "(e.g. probe target at capacity), cooldown restarted", instanceId);
    }

    @Override
    public synchronized void recordSuccess(long now) {
        if (state == CircuitState.HALF_OPEN) {
            halfOpenSuccesses++;
            if (halfOpenSuccesses >= config.trialSuccessRequired()) {
                state = CircuitState.CLOSED;
                consecutiveFailures = 0;
                halfOpenSuccesses = 0;
                openedAtMillis = 0L;
                log.warn("circuit[{}] HALF_OPEN -> CLOSED: trial success ({}/{}), traffic restored",
                        instanceId, halfOpenSuccesses, config.trialSuccessRequired());
            } else {
                log.info("circuit[{}] HALF_OPEN trial success {}/{}, waiting for more",
                        instanceId, halfOpenSuccesses, config.trialSuccessRequired());
            }
        } else if (state == CircuitState.CLOSED) {
            consecutiveFailures = 0;
        }
    }

    @Override
    public synchronized void recordFailure(long now) {
        recordFailure(now, null);
    }

    @Override
    public synchronized void recordFailure(long now, FailureType failureType) {
        if (failureType != null) {
            recentFailures[recentFailureCursor] = failureType;
            recentFailureCursor = (recentFailureCursor + 1) % RECENT_FAILURE_WINDOW;
            recentFailureSize = Math.min(recentFailureSize + 1, RECENT_FAILURE_WINDOW);
        }
        if (state == CircuitState.HALF_OPEN) {
            // 半开探测失败：保持熔断，重新计时
            state = CircuitState.OPEN;
            halfOpenSuccesses = 0;
            openedAtMillis = now;
            log.warn("circuit[{}] HALF_OPEN -> OPEN: trial failed, circuit stays open for another {}ms",
                    instanceId, config.openMillis());
            return;
        }
        consecutiveFailures++;
        if (state == CircuitState.CLOSED && consecutiveFailures >= config.failureThreshold()) {
            state = CircuitState.OPEN;
            openedAtMillis = now;
            log.warn("circuit[{}] CLOSED -> OPEN: consecutive failures reached {}/{}",
                    instanceId, consecutiveFailures, config.failureThreshold());
        } else {
            log.debug("circuit[{}] recorded failure {}/{} (state={})",
                    instanceId, consecutiveFailures, config.failureThreshold(), state);
        }
    }

    /** 最近失败类型分布的只读快照（不改变任何状态）。 */
    public synchronized Map<FailureType, Integer> recentFailureDistribution() {
        Map<FailureType, Integer> distribution = new EnumMap<>(FailureType.class);
        for (int i = 0; i < recentFailureSize; i++) {
            FailureType type = recentFailures[i];
            if (type != null) {
                distribution.merge(type, 1, Integer::sum);
            }
        }
        return Map.copyOf(distribution);
    }

    /** 测试辅助：直接置为 OPEN 并开始计时。 */
    public synchronized void forceOpen(long now) {
        state = CircuitState.OPEN;
        openedAtMillis = now;
        halfOpenSuccesses = 0;
        log.warn("circuit[{}] forced OPEN at {}", instanceId, now);
    }

    /** 测试辅助：直接回到 CLOSED。 */
    public synchronized void forceClosed() {
        state = CircuitState.CLOSED;
        consecutiveFailures = 0;
        halfOpenSuccesses = 0;
        openedAtMillis = 0L;
    }
}
