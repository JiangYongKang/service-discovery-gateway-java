package com.github.highcumontoa.servicediscoverygatewayjava.tests;

import com.github.highcumontoa.servicediscoverygatewayjava.circuit.CircuitBreaker;
import com.github.highcumontoa.servicediscoverygatewayjava.circuit.CircuitState;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 熔断三态与半开恢复：使用可控时钟确定性推进时间。
 */
class CircuitBreakerTest {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerTest.class);

    @Test
    void closedOpenHalfOpenRecoveryAndReject() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(3, 1000L, clock::get);

        log.info("[T] 输入：阈值=3，打开时长=1000ns，初始时钟=0");
        assertEquals(CircuitState.CLOSED, cb.state());

        // 连续 2 次失败：仍关闭，放行
        cb.recordFailure();
        cb.recordFailure();
        log.info("[T] 2 次失败后状态={} 选择结果=allowRequest={} 依据=未达阈值", cb.state(), cb.allowRequest());
        assertEquals(CircuitState.CLOSED, cb.state());
        assertTrue(cb.allowRequest());

        // 第 3 次失败：打开
        cb.recordFailure();
        log.info("[T] 第3次失败后状态={} 依据=达到阈值", cb.state());
        assertEquals(CircuitState.OPEN, cb.state());
        assertFalse(cb.allowRequest(), "熔断打开期间必须快速失败、不放大流量");

        // 时间推进到打开期内：仍拒绝
        clock.addAndGet(500L);
        assertFalse(cb.allowRequest());
        log.info("[T] 时钟+500（未到期）状态={} allowRequest=false 依据=打开期内", cb.state());

        // 时间到期：下一个请求进入 HALF_OPEN 被放行，且仅放行一个
        clock.addAndGet(501L);
        assertTrue(cb.allowRequest(), "打开到期后应允许一个半开探测");
        assertEquals(CircuitState.HALF_OPEN, cb.state());
        assertFalse(cb.allowRequest(), "半开期间其余请求必须被拒绝");
        log.info("[T] 时钟到期，首个探测进入 HALF_OPEN，第二个请求被拒绝");

        // 半开探测失败：回到 OPEN 并重新计时，保持熔断
        cb.recordFailure();
        assertEquals(CircuitState.OPEN, cb.state());
        assertFalse(cb.allowRequest());
        log.info("[T] 半开探测失败后状态={} allowRequest=false 依据=保持熔断", cb.state());

        // 再次到期，探测成功：恢复 CLOSED
        clock.addAndGet(1001L);
        assertTrue(cb.allowRequest());
        assertEquals(CircuitState.HALF_OPEN, cb.state());
        cb.recordSuccess();
        assertEquals(CircuitState.CLOSED, cb.state());
        assertTrue(cb.allowRequest());
        log.info("[T] 再次到期半开探测成功，状态恢复={}", cb.state());
    }

    @Test
    void successResetsFailureCount() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(2, 1000L, clock::get);
        cb.recordFailure();
        cb.recordSuccess();
        cb.recordFailure();
        assertEquals(CircuitState.CLOSED, cb.state(), "成功应清零连续失败计数");
        log.info("[T] 失败-成功-失败后仍 CLOSED，依据=成功清零计数");
    }
}
