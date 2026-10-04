package com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry;

/**
 * 按服务的自动重试预算：限制滑动窗口内的自动重试总量与占比。
 *
 * 约束：
 * - 只拦截自动重试；首发请求不经过预算判定，永远照常发出；
 * - 判定与计数必须并发安全；
 * - 每次判定都给出可解释的依据（窗口内已用量/上限），供日志与结果 reason 使用。
 */
public interface RetryBudget {

    /** 记录一次真实发出的首发请求（每个请求的第 1 次尝试）。 */
    void recordFirstAttempt(String serviceName, long now);

    /**
     * 尝试为一次自动重试消耗预算。
     *
     * @return 判定结果：允许时本次重试计入窗口；拒绝时携带窗口用量与上限的依据
     */
    Decision tryConsumeRetry(String serviceName, long now);

    /** 预算判定结果。 */
    record Decision(boolean allowed, String reason) {

        public static Decision allow(String reason) {
            return new Decision(true, reason);
        }

        public static Decision deny(String reason) {
            return new Decision(false, reason);
        }
    }
}
