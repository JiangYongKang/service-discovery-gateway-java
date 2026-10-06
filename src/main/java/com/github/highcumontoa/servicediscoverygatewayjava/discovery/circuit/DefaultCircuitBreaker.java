package com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * 默认单实例熔断器，所有状态迁移在 synchronized 方法内完成。
 *
 * 状态机：
 * - CLOSED：正常放行；连续失败计数达到 failureThreshold 时迁到 OPEN 并记录打开时刻；
 * - OPEN：拒绝一切请求（快速失败，不向下游放大流量）；距打开超过 openMillis 后冷却到点，
 *   此时真实请求或后台周期探测可通过 {@link #tryAcquireRecoveryProbe} 原子领取唯一探测，
 *   状态翻到 HALF_OPEN；
 * - HALF_OPEN：领取窗口内只允许一个探测请求；探测成功 trialSuccessRequired 次 -> CLOSED；
 *   任一失败 -> 立即回到 OPEN，<strong>重新计时</strong>，等待下一轮探测。
 *
 * 多实例恢复互不影响：每个熔断器独立领取探测；不同实例可同时各自探测。
 * HALF_OPEN 占坑若超过 probeStaleMillis 仍无结果（占坑线程异常退出等），判定探测丢失，
 * 后台探测可重新领取——实例不会永久卡在 HALF_OPEN 而再也等不到探测。
 *
 * 失败类型分布按 {@code failureWindowMillis} 滑动窗口统计：只保留最近一段时间的失败，
 * 视图查询按时间窗过滤，是“最近口径”而非启动以来的历史累计。
 */
public class DefaultCircuitBreaker implements CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(DefaultCircuitBreaker.class);

    private final String instanceId;
    private final CircuitBreakerConfig config;
    private final LongSupplier clock;

    private CircuitState state = CircuitState.CLOSED;
    private int consecutiveFailures = 0;
    private int halfOpenSuccesses = 0;
    private long openedAtMillis = 0L;
    /** 最近一次进入 HALF_OPEN（领取探测）的时刻，用于识别失活占坑。 */
    private long halfOpenEnteredAtMillis = 0L;
    /** 每次失败记录一个时间戳，按类型分桶；查询时按滑动窗口过滤。 */
    private final EnumMap<FailureType, Deque<Long>> failureEvents = new EnumMap<>(FailureType.class);

    public DefaultCircuitBreaker(String instanceId, CircuitBreakerConfig config) {
        this(instanceId, config, System::currentTimeMillis);
    }

    public DefaultCircuitBreaker(String instanceId, CircuitBreakerConfig config, LongSupplier clock) {
        this.instanceId = instanceId;
        this.config = config;
        this.clock = clock;
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
    public synchronized boolean isRecoveryProbeDue(long now) {
        return switch (state) {
            case OPEN -> now - openedAtMillis >= config.openMillis();
            // 半开占坑失活：探测迟迟没有结果返回，允许重新领取一次
            case HALF_OPEN -> now - halfOpenEnteredAtMillis >= config.probeStaleMillis();
            default -> false;
        };
    }

    @Override
    public synchronized boolean tryAcquireRecoveryProbe(long now) {
        if (state == CircuitState.OPEN && now - openedAtMillis >= config.openMillis()) {
            enterHalfOpen(now);
            log.info("circuit[{}] OPEN -> HALF_OPEN: cooldown elapsed={}ms >= openMillis={}ms, "
                            + "recovery probe claimed (exactly ONE in flight)",
                    instanceId, now - openedAtMillis, config.openMillis());
            return true;
        }
        if (state == CircuitState.HALF_OPEN
                && now - halfOpenEnteredAtMillis >= config.probeStaleMillis()) {
            // 占坑超时失活：原子地把领取时刻刷新为当前，等价于重新领取唯一探测
            long staleFor = now - halfOpenEnteredAtMillis;
            halfOpenSuccesses = 0;
            halfOpenEnteredAtMillis = now;
            log.warn("circuit[{}] stale HALF_OPEN probe reclaimed: no result for {}ms >= {}ms, "
                            + "a new recovery probe is claimed",
                    instanceId, staleFor, config.probeStaleMillis());
            return true;
        }
        return false;
    }

    private void enterHalfOpen(long now) {
        state = CircuitState.HALF_OPEN;
        halfOpenSuccesses = 0;
        halfOpenEnteredAtMillis = now;
    }

    @Override
    public synchronized boolean allowRequest(long now) {
        switch (state) {
            case CLOSED -> {
                return true;
            }
            case OPEN -> {
                long elapsed = now - openedAtMillis;
                if (elapsed >= config.openMillis()) {
                    // 原子放行唯一探测请求：状态立刻变 HALF_OPEN，后续请求继续被拒
                    enterHalfOpen(now);
                    log.info("circuit[{}] OPEN -> HALF_OPEN: elapsed={}ms >= openMillis={}ms, "
                            + "exactly ONE trial request allowed", instanceId, elapsed, config.openMillis());
                    return true;
                }
                log.debug("circuit[{}] reject: state=OPEN, remaining={}ms",
                        instanceId, config.openMillis() - elapsed);
                return false;
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
    public synchronized void recordSuccess(long now) {
        if (state == CircuitState.HALF_OPEN) {
            halfOpenSuccesses++;
            if (halfOpenSuccesses >= config.trialSuccessRequired()) {
                state = CircuitState.CLOSED;
                consecutiveFailures = 0;
                halfOpenSuccesses = 0;
                openedAtMillis = 0L;
                halfOpenEnteredAtMillis = 0L;
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
    public synchronized void recordFailure(long now, FailureType type) {
        if (type != null) {
            // 写入路径按“当前时刻”清理窗口外旧事件，使事件队列长度有界；
            // 只读视图的查询不做任何清理（查看不改变状态）。
            pruneWindow(clock.getAsLong());
            failureEvents.computeIfAbsent(type, k -> new ArrayDeque<>()).addLast(now);
        }
        if (state == CircuitState.HALF_OPEN) {
            // 半开探测失败：保持熔断，重新计时
            state = CircuitState.OPEN;
            halfOpenSuccesses = 0;
            halfOpenEnteredAtMillis = 0L;
            openedAtMillis = now;
            log.warn("circuit[{}] HALF_OPEN -> OPEN: trial failed, circuit stays open for another {}ms",
                    instanceId, config.openMillis());
            return;
        }
        consecutiveFailures++;
        if (state == CircuitState.CLOSED && consecutiveFailures >= config.failureThreshold()) {
            state = CircuitState.OPEN;
            openedAtMillis = now;
            halfOpenEnteredAtMillis = 0L;
            log.warn("circuit[{}] CLOSED -> OPEN: consecutive failures reached {}/{}",
                    instanceId, consecutiveFailures, config.failureThreshold());
        } else {
            log.debug("circuit[{}] recorded failure {}/{} (state={})",
                    instanceId, consecutiveFailures, config.failureThreshold(), state);
        }
    }

    /** 写入路径顺手清理窗口外时间戳，保证事件队列长度有界（只读视图不做任何清理）。 */
    private void pruneWindow(long now) {
        long cutoff = now - config.failureWindowMillis();
        for (Deque<Long> events : failureEvents.values()) {
            while (!events.isEmpty() && events.peekFirst() < cutoff) {
                events.pollFirst();
            }
        }
    }

    /**
     * 只读：最近 failureWindowMillis 时间窗内的失败类型分布快照（拷贝，不影响内部状态）。
     * 当前时间取注入时钟。
     */
    @Override
    public Map<FailureType, Integer> failureTypeDistribution() {
        return failureTypeDistribution(clock.getAsLong());
    }

    /** 只读：按给定 now 过滤窗口内失败，不增删任何记录、不改变熔断状态。 */
    @Override
    public synchronized Map<FailureType, Integer> failureTypeDistribution(long now) {
        long cutoff = now - config.failureWindowMillis();
        EnumMap<FailureType, Integer> snapshot = new EnumMap<>(FailureType.class);
        failureEvents.forEach((type, events) -> {
            int count = 0;
            for (long ts : events) {
                if (ts >= cutoff) {
                    count++;
                }
            }
            if (count > 0) {
                snapshot.put(type, count);
            }
        });
        return Collections.unmodifiableMap(snapshot);
    }

    /** 测试辅助：直接置为 OPEN 并开始计时。 */
    public synchronized void forceOpen(long now) {
        state = CircuitState.OPEN;
        openedAtMillis = now;
        halfOpenSuccesses = 0;
        halfOpenEnteredAtMillis = 0L;
        log.warn("circuit[{}] forced OPEN at {}", instanceId, now);
    }

    /** 测试辅助：直接回到 CLOSED。 */
    public synchronized void forceClosed() {
        state = CircuitState.CLOSED;
        consecutiveFailures = 0;
        halfOpenSuccesses = 0;
        openedAtMillis = 0L;
        halfOpenEnteredAtMillis = 0L;
    }
}
