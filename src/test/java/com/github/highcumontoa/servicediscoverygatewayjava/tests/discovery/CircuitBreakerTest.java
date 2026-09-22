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
}
