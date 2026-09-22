package com.github.highcumontoa.servicediscoverygatewayjava.config;

import com.github.highcumontoa.servicediscoverygatewayjava.downstream.DownstreamTransport;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.InMemoryDownstreamTransport;
import com.github.highcumontoa.servicediscoverygatewayjava.gateway.ForwardingEngine;
import com.github.highcumontoa.servicediscoverygatewayjava.health.HealthChecker;
import com.github.highcumontoa.servicediscoverygatewayjava.idempotency.IdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer.LoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer.LoadBalancerFactory;
import com.github.highcumontoa.servicediscoverygatewayjava.registry.ServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.retry.RetryPolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayConfiguration {

    @Bean
    public DownstreamTransport downstreamTransport() {
        return new InMemoryDownstreamTransport();
    }

    @Bean
    public ServiceRegistry serviceRegistry(GatewayProperties props) {
        return new ServiceRegistry(
                props.getCircuitFailureThreshold(),
                TimeUnit.MILLISECONDS.toNanos(props.getCircuitOpenDurationMillis()),
                System::nanoTime);
    }

    @Bean
    public LoadBalancer loadBalancer(GatewayProperties props) {
        return LoadBalancerFactory.create(props.getPolicy());
    }

    @Bean
    public IdempotencyDeduplicator idempotencyDeduplicator() {
        return new IdempotencyDeduplicator();
    }

    @Bean
    public RetryPolicy retryPolicy(GatewayProperties props) {
        return new RetryPolicy(props.getMaxAttempts(), props.getRetryBackoffMillis());
    }

    @Bean
    public ExecutorService gatewayExecutor() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    public ScheduledExecutorService healthScheduler() {
        return Executors.newSingleThreadScheduledExecutor();
    }

    @Bean
    public HealthChecker healthChecker(GatewayProperties props, ServiceRegistry registry,
                                       DownstreamTransport transport, ScheduledExecutorService scheduler) {
        HealthChecker checker = new HealthChecker(
                registry, transport,
                props.getHealthFailureThreshold(),
                props.getHealthProbeIntervalMillis(),
                props.getHealthTimeoutMillis(),
                props.getEvictDurationMillis(),
                scheduler);
        if (props.isHealthCheckEnabled()) {
            checker.start();
        }
        return checker;
    }

    @Bean
    public ForwardingEngine forwardingEngine(ServiceRegistry registry, DownstreamTransport transport,
                                             LoadBalancer loadBalancer, RetryPolicy retryPolicy,
                                             IdempotencyDeduplicator deduplicator,
                                             GatewayProperties props) {
        return new ForwardingEngine(registry, transport, loadBalancer, retryPolicy,
                deduplicator, props.getTimeoutMillis());
    }
}
