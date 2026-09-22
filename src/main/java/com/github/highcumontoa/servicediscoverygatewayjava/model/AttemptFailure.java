package com.github.highcumontoa.servicediscoverygatewayjava.model;

/**
 * 单次尝试失败的诊断信息（用于日志与测试判定）。
 */
public record AttemptFailure(int attempt, String instanceId, FailureType failureType, String reason) {
}
