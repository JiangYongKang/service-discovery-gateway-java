package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 网关转发过程中的统一异常，携带可区分的失败类型。
 */
public class GatewayException extends RuntimeException {

    private final FailureType failureType;

    public GatewayException(FailureType failureType, String message) {
        super(message);
        this.failureType = failureType;
    }

    public GatewayException(FailureType failureType, String message, Throwable cause) {
        super(message, cause);
        this.failureType = failureType;
    }

    public FailureType getFailureType() {
        return failureType;
    }
}
