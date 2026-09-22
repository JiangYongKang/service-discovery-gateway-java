package com.github.highcumontoa.servicediscoverygatewayjava.downstream;

import com.github.highcumontoa.servicediscoverygatewayjava.model.DownstreamResponse;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ProbeResult;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;

/**
 * 进程内下游传输层：用模拟实例集群替代真实网络，
 * 但仍然抛出可区分 TIMEOUT / CONNECTION_REFUSED / DOWNSTREAM_ERROR 的异常。
 */
public interface DownstreamTransport {

    void register(SimulatedInstance instance);

    SimulatedInstance instance(String instanceId);

    DownstreamResponse invoke(ServiceInstance instance, String payload, long timeoutMillis);

    ProbeResult probe(ServiceInstance instance, long timeoutMillis);
}
