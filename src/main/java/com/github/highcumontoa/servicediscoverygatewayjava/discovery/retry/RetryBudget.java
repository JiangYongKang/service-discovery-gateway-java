package com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry;

/**
 * 按服务的自动重试预算（滑动窗口）。
 *
 * 并发安全；判定只拦截“自动重试”，首发请求永远放行。
 */
public interface RetryBudget {

    /** 记录一次首发请求（用于重试占比统计）。 */
    void recordRequest(String serviceName, long now);

    /**
     * 尝试为一次自动重试占用预算。
     *
     * @return 判定结果：是否允许 + 判定依据（用于日志）
     */
    RetryDecision tryAcquireRetry(String serviceName, long now);

    /** 重试预算判定结果。 */
    record RetryDecision(boolean allowed, String reason) {
    }
}
