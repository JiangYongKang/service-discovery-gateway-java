package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 网关侧可区分的失败类型。
 */
public enum FailureType {
    /** 下游处理超时 */
    TIMEOUT,
    /** 下游连接被拒绝（实例不可达） */
    CONNECTION_REFUSED,
    /** 下游返回错误状态码（如 5xx） */
    DOWNSTREAM_ERROR,
    /** 当前没有任何健康实例可选 */
    NO_HEALTHY_INSTANCE,
    /** 实例熔断器打开，请求被快速失败 */
    CIRCUIT_OPEN,
    /** 可重试请求在限定次数内仍未成功 */
    RETRY_EXHAUSTED,
    /** 请求被标记为不可安全重试，因此发生失败后不做重试 */
    NON_RETRYABLE_REQUEST
}
