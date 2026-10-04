package com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryBudgetPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 滑动时间窗重试预算实现。
 *
 * - 每个服务独立维护窗口内的首发请求时间戳与自动重试时间戳；
 * - 一次自动重试被允许，当且仅当：
 *   窗口内已发生重试数 < maxRetriesInWindow，
 *   且放行本次后 重试数/首发数 仍 <= maxRetryRatio；
 * - 判定与记账在同一 synchronized 块内完成，并发下不会超发预算；
 * - 窗口外的旧时间戳在每次访问时惰性清理，窗口滑过后预算自然恢复；
 * - 预算只拦截自动重试：首发请求永远不在此判定路径上。
 */
public class SlidingWindowRetryBudget implements RetryBudget {

    private static final Logger log = LoggerFactory.getLogger(SlidingWindowRetryBudget.class);

    private final RetryBudgetPolicy policy;
    private final Map<String, ServiceWindow> windows = new ConcurrentHashMap<>();

    public SlidingWindowRetryBudget(RetryBudgetPolicy policy) {
        this.policy = policy;
    }

    @Override
    public void recordRequest(String serviceName, long now) {
        if (!policy.enabled()) {
            return;
        }
        ServiceWindow window = windows.computeIfAbsent(serviceName, k -> new ServiceWindow());
        synchronized (window) {
            window.prune(now, policy.windowMillis());
            window.requests.addLast(now);
        }
    }

    @Override
    public RetryDecision tryAcquireRetry(String serviceName, long now) {
        if (!policy.enabled()) {
            return new RetryDecision(true, "retry budget disabled: legacy rules only");
        }
        ServiceWindow window = windows.computeIfAbsent(serviceName, k -> new ServiceWindow());
        synchronized (window) {
            window.prune(now, policy.windowMillis());
            int retries = window.retries.size();
            int requests = window.requests.size();
            if (retries >= policy.maxRetriesInWindow()) {
                String reason = "retry budget exhausted: retries=" + retries + "/"
                        + policy.maxRetriesInWindow() + " within window=" + policy.windowMillis()
                        + "ms; first attempt still allowed, auto-retry suppressed";
                log.info("retry budget decision: service={} -> DENY ({})", serviceName, reason);
                return new RetryDecision(false, reason);
            }
            double ratioAfter = (retries + 1.0) / Math.max(1, requests);
            // maxRetryRatio >= 1.0 视为不限制占比（重试数本就可能超过首发数）
            if (policy.maxRetryRatio() < 1.0 && ratioAfter > policy.maxRetryRatio()) {
                String reason = String.format(
                        "retry ratio budget exhausted: ratioAfter=%.2f > max=%.2f "
                                + "(retries=%d, requests=%d, window=%dms); auto-retry suppressed",
                        ratioAfter, policy.maxRetryRatio(), retries, requests, policy.windowMillis());
                log.info("retry budget decision: service={} -> DENY ({})", serviceName, reason);
                return new RetryDecision(false, reason);
            }
            window.retries.addLast(now);
            String reason = "within retry budget: retries=" + (retries + 1) + "/"
                    + policy.maxRetriesInWindow() + ", ratio="
                    + String.format("%.2f", ratioAfter) + " <= " + policy.maxRetryRatio()
                    + " (window=" + policy.windowMillis() + "ms)";
            log.info("retry budget decision: service={} -> ALLOW ({})", serviceName, reason);
            return new RetryDecision(true, reason);
        }
    }

    /** 单服务滑动窗口：窗口外的旧事件在访问时惰性剔除。 */
    private static final class ServiceWindow {
        private final Deque<Long> requests = new ArrayDeque<>();
        private final Deque<Long> retries = new ArrayDeque<>();

        void prune(long now, long windowMillis) {
            long cutoff = now - windowMillis;
            while (!requests.isEmpty() && requests.peekFirst() <= cutoff) {
                requests.pollFirst();
            }
            while (!retries.isEmpty() && retries.peekFirst() <= cutoff) {
                retries.pollFirst();
            }
        }
    }
}
