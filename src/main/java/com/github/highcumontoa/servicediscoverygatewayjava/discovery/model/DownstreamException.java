package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/** 下游调用异常：携带可区分的失败类型。 */
public class DownstreamException extends RuntimeException {

    private final FailureType failureType;

    public DownstreamException(FailureType failureType, String message) {
        super(message);
        this.failureType = failureType;
    }

    public DownstreamException(FailureType failureType, String message, Throwable cause) {
        super(message, cause);
        this.failureType = failureType;
    }

    public FailureType failureType() {
        return failureType;
    }
}
