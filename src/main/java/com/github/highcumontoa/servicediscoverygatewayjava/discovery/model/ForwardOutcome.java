package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

import java.util.List;

/** 转发结果：成功标志、响应、失败类型、判定依据与各次尝试明细。 */
public record ForwardOutcome(boolean success,
                             DownstreamResponse response,
                             FailureType failureType,
                             String reason,
                             List<AttemptRecord> attempts) {

    public record AttemptRecord(String instanceId, int attempt, String result) {
    }
}
