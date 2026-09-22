package com.github.highcumontoa.servicediscoverygatewayjava.retry;

import com.github.highcumontoa.servicediscoverygatewayjava.model.FailureType;

import java.util.Set;

/**
 * 重试策略配置：最大尝试次数（含首次）与重试间隔。
 *
 * 可安全重试（safeRetry=true，如 GET / 幂等请求）且失败类型属于可重试集合时，才允许再试一次。
 * 熔断打开、无健康实例属于“快速失败”信号，不重试；不可安全重试的请求绝不静默重试。
 */
public record RetryPolicy(int maxAttempts, long backoffMillis) {

    /** 值得换实例再试一次的下游故障；熔断/无实例不在其中，避免放大压力。 */
    private static final Set<FailureType> RETRYABLE = Set.of(
            FailureType.TIMEOUT,
            FailureType.CONNECTION_REFUSED,
            FailureType.DOWNSTREAM_ERROR);

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (backoffMillis < 0) {
            throw new IllegalArgumentException("backoffMillis must be >= 0");
        }
    }

    /** 只有可安全重试的请求、失败类型可重试、且仍有剩余次数时才允许再试一次。 */
    public boolean canRetry(boolean safeRetry, FailureType failureType, int attemptsDone) {
        return safeRetry && attemptsDone < maxAttempts && RETRYABLE.contains(failureType);
    }
}
