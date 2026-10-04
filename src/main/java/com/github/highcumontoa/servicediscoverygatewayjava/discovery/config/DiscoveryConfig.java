package com.github.highcumontoa.servicediscoverygatewayjava.discovery.config;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.DownstreamHandler;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.SimulatedDownstream;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway.GatewayForwarder;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.HealthProbeScheduler;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.InMemoryServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.LoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.WeightedRoundRobinLoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.HealthConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.RetryBudget;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.RetryBudgetConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.SlidingWindowRetryBudget;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 将模拟器、注册中心、负载均衡、熔断器与转发器装配为 Spring Bean。 */
@Configuration
public class DiscoveryConfig {

    public static final String DEMO_SERVICE = "demo-service";

    @Bean
    public SimulatedDownstream simulatedDownstream() {
        // TIMEOUT 模式真实等待 120ms，演示时肉眼可辨；请求超时阈值取 200ms
        return new SimulatedDownstream(120L);
    }

    @Bean
    public HealthConfig healthConfig() {
        // 连续 3 次探测失败摘除；摘除 3 秒冷却后才允许复活探测；每 500ms 探一轮；探测超时 200ms
        return new HealthConfig(3, 3000, 500, 200);
    }

    @Bean
    public CircuitBreakerConfig circuitBreakerConfig() {
        // 连续 3 次真实调用失败熔断；熔断 2 秒后半开；半开探测 1 次成功即恢复
        return new CircuitBreakerConfig(3, 2000, 1);
    }

    @Bean
    public RetryPolicy retryPolicy() {
        // 最多 3 次尝试（1 次正常 + 2 次重试），线性退避 50ms*attempt，限制下游压力放大
        return new RetryPolicy(3, 50);
    }

    @Bean
    public RetryBudgetConfig retryBudgetConfig() {
        // 默认不启用重试预算（与既有行为一致）；本地验证时可改为
        // new RetryBudgetConfig(10_000, 20, 0.5) 之类的窗口限制
        return RetryBudgetConfig.disabled();
    }

    @Bean
    public RetryBudget retryBudget(RetryBudgetConfig retryBudgetConfig) {
        return new SlidingWindowRetryBudget(retryBudgetConfig);
    }

    @Bean
    public ServiceRegistry serviceRegistry(HealthConfig healthConfig, DownstreamHandler downstream) {
        return new InMemoryServiceRegistry(healthConfig, downstream);
    }

    @Bean
    public LoadBalancer loadBalancer() {
        // 平滑加权轮询：等权重（默认）时严格退化为普通轮询，行为与之前一致
        return new WeightedRoundRobinLoadBalancer();
    }

    @Bean
    public GatewayForwarder gatewayForwarder(ServiceRegistry registry,
                                             LoadBalancer loadBalancer,
                                             DownstreamHandler downstream,
                                             RetryPolicy retryPolicy,
                                             CircuitBreakerConfig circuitConfig,
                                             RetryBudget retryBudget) {
        return new GatewayForwarder(registry, loadBalancer, downstream, retryPolicy,
                circuitConfig, System::currentTimeMillis,
                registry.config().probeTimeoutMillis(), millis -> {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, retryBudget);
    }

    @Bean
    public HealthProbeScheduler healthProbeScheduler(ServiceRegistry registry, HealthConfig healthConfig) {
        HealthProbeScheduler scheduler = new HealthProbeScheduler(registry);
        scheduler.start(DEMO_SERVICE, healthConfig.probeIntervalMillis());
        return scheduler;
    }

    @Bean
    public SchedulerLifecycle schedulerLifecycle(HealthProbeScheduler scheduler) {
        return new SchedulerLifecycle(scheduler);
    }

    /** 应用关闭时停止探测线程池。 */
    public static class SchedulerLifecycle {
        private final HealthProbeScheduler scheduler;

        public SchedulerLifecycle(HealthProbeScheduler scheduler) {
            this.scheduler = scheduler;
        }

        @PreDestroy
        public void stop() {
            scheduler.stop();
        }
    }
}
