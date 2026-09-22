package com.github.highcumontoa.servicediscoverygatewayjava.discovery.model;

import java.util.List;

/** 某服务的实例快照（供日志与调试解释当前状态）。 */
public record ServiceSnapshot(String serviceName, List<String> healthyIds, List<String> unhealthyIds) {
}
