package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/**
 * 按服务的自动重试预算：滑动时间窗内的重试总量上限与重试占比上限。
 *
 * 只约束“自动重试”，不约束首发请求：
 * - 预算耗尽时，新请求照常发首发，只是不再自动换实例重试；
 * - 窗口滑过后预算自动恢复。
 *
 * 默认 {@link #disabled()}：不启用预算，行为与既有版本完全一致。
 *
 * @param windowMillis        滑动窗口长度（毫秒）
 * @param maxRetriesInWindow  窗口内允许的自动重试总次数（<=0 表示不启用）
 * @param maxRetryRatio       窗口内 重试次数/首发请求次数 的上限（[0,1]；1.0 表示不限制占比）
 */
public record RetryBudgetPolicy(long windowMillis,
                                int maxRetriesInWindow,
                                double maxRetryRatio) {

    /** 不启用重试预算（默认）：所有既有重试规则保持不变。 */
    public static RetryBudgetPolicy disabled() {
        return new RetryBudgetPolicy(0L, 0, 1.0);
    }

    public boolean enabled() {
        return windowMillis > 0 && maxRetriesInWindow > 0;
    }
}
