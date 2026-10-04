package com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry;

/**
 * 按服务的自动重试预算（滑动时间窗）。
 *
 * - windowMillis：滑动窗口长度；
 * - maxRetriesPerWindow：窗口内允许的自动重试总量上限，&lt;0 表示不限制；
 * - maxRetryRatio：窗口内 重试次数/首发次数 的占比上限（如 0.2 表示重试不超过首发的 20%），
 *   &lt;0 表示不限制。
 *
 * 两个维度都可独立开关；默认 {@link #disabled()} 两个维度都关闭，
 * 行为与未引入预算时完全一致。预算只拦截“自动重试”，首发请求永远照常发出。
 */
public record RetryBudgetConfig(long windowMillis,
                                int maxRetriesPerWindow,
                                double maxRetryRatio) {

    public RetryBudgetConfig {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be positive: " + windowMillis);
        }
    }

    /** 默认：不做任何重试预算限制，保持既有行为。 */
    public static RetryBudgetConfig disabled() {
        return new RetryBudgetConfig(1000, -1, -1);
    }

    public boolean limitsTotal() {
        return maxRetriesPerWindow >= 0;
    }

    public boolean limitsRatio() {
        return maxRetryRatio >= 0;
    }

    public boolean isDisabled() {
        return !limitsTotal() && !limitsRatio();
    }
}
