package com.github.highcumontoa.servicediscoverygatewayjava.tests.discovery;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.DefaultCircuitBreaker;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 熔断状态机测试：
 * CLOSED -> OPEN（达阈值）-> OPEN 快速失败 -> 冷却后唯一半开探测
 * -> 探测成功恢复 CLOSED；探测失败保持 OPEN 并重新计时。
 */
class CircuitBreakerTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerTest.class);

    private final AtomicLong clock = new AtomicLong(1000);
    private final CircuitBreakerConfig config = new CircuitBreakerConfig(3, 2000, 1);

    @Test
    void opensAfterThresholdAndRejectsWhileOpen() {
        var cb = new DefaultCircuitBreaker("i1", config);
        cb.recordFailure(clock.get());
        cb.recordFailure(clock.get());
        boolean openedByThird = false;
        cb.recordFailure(clock.get());
        openedByThird = cb.state() == CircuitState.OPEN;
        log.info("input: 3 consecutive failures (threshold=3); result: state={}; "
                + "basis=threshold reached => OPEN", cb.state());
        assertTrue(openedByThird);

        boolean rejected = cb.allowRequest(clock.get());
        log.info("input: allowRequest at OPEN, t=1000; result={}; basis=OPEN fail-fast", rejected);
        assertFalse(rejected, "熔断打开期间必须快速失败，不放大流量");
    }

    @Test
    void halfOpenTrialSucceedsThenRecovers() {
        var cb = new DefaultCircuitBreaker("i1", config);
        for (int i = 0; i < 3; i++) {
            cb.recordFailure(clock.get());
        }
        clock.addAndGet(2000); // 冷却结束
        boolean trialAllowed = cb.allowRequest(clock.get());
        CircuitState duringTrial = cb.state();
        boolean secondRequestRejected = cb.allowRequest(clock.get());
        log.info("input: cooldown elapsed; result: trialAllowed={}, stateDuringTrial={}, "
                        + "secondRequestRejected={}; basis=exactly one trial in HALF_OPEN",
                trialAllowed, duringTrial, !secondRequestRejected);
        assertTrue(trialAllowed);
        assertEquals(CircuitState.HALF_OPEN, duringTrial);
        assertFalse(secondRequestRejected, "半开探测进行中，其它请求仍需快速失败");

        cb.recordSuccess(clock.get());
        log.info("input: trial success; result: state={}; basis=trialSuccessRequired=1 => CLOSED", cb.state());
        assertEquals(CircuitState.CLOSED, cb.state());
        assertTrue(cb.allowRequest(clock.get()), "恢复后应重新放行流量");
    }

    @Test
    void halfOpenTrialFailureKeepsCircuitOpen() {
        var cb = new DefaultCircuitBreaker("i1", config);
        for (int i = 0; i < 3; i++) {
            cb.recordFailure(clock.get());
        }
        clock.addAndGet(2000);
        assertTrue(cb.allowRequest(clock.get()));
        cb.recordFailure(clock.get());
        log.info("input: half-open trial failed; result: state={}; basis=failed trial => back to OPEN, "
                + "timer restarted", cb.state());
        assertEquals(CircuitState.OPEN, cb.state(), "探测失败必须保持熔断");

        boolean rejectedImmediately = cb.allowRequest(clock.get());
        clock.addAndGet(1999);
        boolean rejectedBeforeNewCooldown = cb.allowRequest(clock.get());
        log.info("input: allowRequest right after fail and 1999ms later (<2000ms); result={},{}; "
                + "basis=new cooldown must elapse before next trial",
                rejectedImmediately, rejectedBeforeNewCooldown);
        assertFalse(rejectedImmediately);
        assertFalse(rejectedBeforeNewCooldown);

        clock.addAndGet(1);
        assertTrue(cb.allowRequest(clock.get()), "重新计时满 2000ms 后才允许下一次半开探测");
    }

    @Test
    void successResetsFailureCountInClosedState() {
        var cb = new DefaultCircuitBreaker("i1", config);
        cb.recordFailure(clock.get());
        cb.recordFailure(clock.get());
        cb.recordSuccess(clock.get());
        cb.recordFailure(clock.get());
        cb.recordFailure(clock.get());
        log.info("input: 2 fails, 1 success(reset), 2 fails; result: state={}; "
                + "basis=consecutive counter reset by success", cb.state());
        assertEquals(CircuitState.CLOSED, cb.state(), "未连续达到阈值不应熔断");
    }

    @Test
    void probeDueIsReadOnlyAndTryAcquireProbeGrantsSingleWinner() {
        var cb = new DefaultCircuitBreaker("i1", config);
        for (int i = 0; i < 3; i++) {
            cb.recordFailure(clock.get());
        }
        clock.addAndGet(2000);

        // probeDue 只读：反复调用不翻牌
        assertTrue(cb.probeDue(clock.get()));
        assertTrue(cb.probeDue(clock.get()));
        assertEquals(CircuitState.OPEN, cb.state(), "probeDue 不得改变状态");

        // tryAcquireProbe 原子：同一探测窗口只有一个胜出者
        boolean first = cb.tryAcquireProbe(clock.get());
        boolean second = cb.tryAcquireProbe(clock.get());
        log.info("input: cooldown elapsed, two tryAcquireProbe calls; result: first={}, second={}, "
                + "state={}; basis=exactly one probe per window", first, second, cb.state());
        assertTrue(first);
        assertFalse(second, "同一探测窗口内只能有一个探测胜出");
        assertEquals(CircuitState.HALF_OPEN, cb.state());

        // 探测失败 -> 重新 OPEN 并重计时：新冷却期内 probeDue=false、tryAcquireProbe 失败
        cb.recordFailure(clock.get());
        assertEquals(CircuitState.OPEN, cb.state());
        assertFalse(cb.probeDue(clock.get()), "重新计时后新冷却期内不得再到点");
        assertFalse(cb.tryAcquireProbe(clock.get()));
        clock.addAndGet(2000);
        assertTrue(cb.probeDue(clock.get()), "新冷却期满后再次到点");
        assertTrue(cb.tryAcquireProbe(clock.get()));
    }

    @Test
    void failureTypeDistributionCoversOnlyRecentWindow() {
        var windowed = new CircuitBreakerConfig(3, 2000, 1, 5000);
        var cb = new DefaultCircuitBreaker("i1", windowed);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava
                .discovery.model.FailureType.TIMEOUT);
        cb.recordFailure(clock.get(), com.github.highcumontoa.servicediscoverygatewayjava
                .discovery.model.FailureType.CONNECTION_REFUSED);

        var inWindow = cb.failureTypeDistribution(clock.get());
        log.info("input: 2 typed failures at t=1000, window=5000ms; result: dist={}; "
                + "basis=in-window failures counted", inWindow);
        assertEquals(1, inWindow.get(com.github.highcumontoa.servicediscoverygatewayjava
                .discovery.model.FailureType.TIMEOUT));
        assertEquals(1, inWindow.get(com.github.highcumontoa.servicediscoverygatewayjava
                .discovery.model.FailureType.CONNECTION_REFUSED));

        // 越过窗口：旧失败不再计入；读取本身不改变状态
        clock.addAndGet(5001);
        var outOfWindow = cb.failureTypeDistribution(clock.get());
        log.info("input: clock advanced beyond window; result: dist={}, state={}; "
                + "basis=recent-window semantics, read-only view", outOfWindow, cb.state());
        assertTrue(outOfWindow.isEmpty(), "窗口外的历史失败不得计入分布（不是只增累计）");
        assertEquals(CircuitState.CLOSED, cb.state(), "查看分布不得改变熔断状态");
    }
}
