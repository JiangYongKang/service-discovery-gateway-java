package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

/** 可区分的下游失败类型。 */
public enum FailureType {
    TIMEOUT,
    CONNECTION_REFUSED,
    DOWNSTREAM_ERROR,
    NO_HEALTHY_INSTANCE,
    CIRCUIT_OPEN,
    ALL_AT_CAPACITY,
    RETRY_EXHAUSTED
}
