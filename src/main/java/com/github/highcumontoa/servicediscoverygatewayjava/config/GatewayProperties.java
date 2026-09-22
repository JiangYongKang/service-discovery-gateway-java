package com.github.highcumontoa.servicediscoverygatewayjava.config;

import com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer.LoadBalancingPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关配置项（application.properties 中以 gateway.* 覆盖）。
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private LoadBalancingPolicy policy = LoadBalancingPolicy.ROUND_ROBIN;
    private long timeoutMillis = 1000;
    private int maxAttempts = 3;
    private long retryBackoffMillis = 50;
    private int circuitFailureThreshold = 3;
    private long circuitOpenDurationMillis = 2000;
    private int healthFailureThreshold = 2;
    private long healthProbeIntervalMillis = 1000;
    private long healthTimeoutMillis = 500;
    private long evictDurationMillis = 5000;
    private boolean healthCheckEnabled = true;

    public LoadBalancingPolicy getPolicy() { return policy; }
    public void setPolicy(LoadBalancingPolicy policy) { this.policy = policy; }
    public long getTimeoutMillis() { return timeoutMillis; }
    public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(long retryBackoffMillis) { this.retryBackoffMillis = retryBackoffMillis; }
    public int getCircuitFailureThreshold() { return circuitFailureThreshold; }
    public void setCircuitFailureThreshold(int circuitFailureThreshold) {
        this.circuitFailureThreshold = circuitFailureThreshold;
    }
    public long getCircuitOpenDurationMillis() { return circuitOpenDurationMillis; }
    public void setCircuitOpenDurationMillis(long circuitOpenDurationMillis) {
        this.circuitOpenDurationMillis = circuitOpenDurationMillis;
    }
    public int getHealthFailureThreshold() { return healthFailureThreshold; }
    public void setHealthFailureThreshold(int healthFailureThreshold) {
        this.healthFailureThreshold = healthFailureThreshold;
    }
    public long getHealthProbeIntervalMillis() { return healthProbeIntervalMillis; }
    public void setHealthProbeIntervalMillis(long healthProbeIntervalMillis) {
        this.healthProbeIntervalMillis = healthProbeIntervalMillis;
    }
    public long getHealthTimeoutMillis() { return healthTimeoutMillis; }
    public void setHealthTimeoutMillis(long healthTimeoutMillis) { this.healthTimeoutMillis = healthTimeoutMillis; }
    public long getEvictDurationMillis() { return evictDurationMillis; }
    public void setEvictDurationMillis(long evictDurationMillis) { this.evictDurationMillis = evictDurationMillis; }
    public boolean isHealthCheckEnabled() { return healthCheckEnabled; }
    public void setHealthCheckEnabled(boolean healthCheckEnabled) {
        this.healthCheckEnabled = healthCheckEnabled;
    }
}
