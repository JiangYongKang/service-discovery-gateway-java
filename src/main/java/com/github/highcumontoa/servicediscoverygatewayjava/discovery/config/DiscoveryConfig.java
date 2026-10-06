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
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryBudgetPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.RetryBudget;
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
        // 连续 3 次真实调用失败熔断；熔断 2 秒后允许半开探测；半开探测 1 次成功即恢复；
        // 半开探测 2 秒无结果视为占坑丢失，后台恢复扫描会重新领取（防止实例永久卡在探测中）；
        // 只读状态视图的失败类型分布统计最近 10 秒。
        return new CircuitBreakerConfig(3, 2000, 1, 2000, 10_000);
    }

    @Bean
    public RetryPolicy retryPolicy() {
        // 最多 3 次尝试（1 次正常 + 2 次重试），线性退避 50ms*attempt，限制下游压力放大
        return new RetryPolicy(3, 50);
    }

    @Bean
    public RetryBudgetPolicy retryBudgetPolicy() {
        // 默认不启用重试预算：行为与既有版本完全一致。
        // 需要容量治理时改为例如 new RetryBudgetPolicy(10_000, 20, 0.2)：
        // 10 秒滑窗内自动重试不超过 20 次且不超过首发量的 20%。
        return RetryBudgetPolicy.disabled();
    }

    @Bean
    public RetryBudget retryBudget(RetryBudgetPolicy retryBudgetPolicy) {
        return new SlidingWindowRetryBudget(retryBudgetPolicy);
    }

    @Bean
    public ServiceRegistry serviceRegistry(HealthConfig healthConfig, DownstreamHandler downstream) {
        return new InMemoryServiceRegistry(healthConfig, downstream);
    }

    @Bean
    public LoadBalancer loadBalancer() {
        // 平滑加权轮询：注册时未指定权重（默认 1）且不限并发时，退化为普通轮询，
        // 与既有默认行为一致；指定权重后按权重分摊，满载实例本轮自动让出份额。
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
    public HealthProbeScheduler healthProbeScheduler(ServiceRegistry registry,
                                                     HealthConfig healthConfig,
                                                     GatewayForwarder gatewayForwarder) {
        HealthProbeScheduler scheduler = new HealthProbeScheduler(registry);
        // 每轮健康探测后，再对所有冷却到点/探测失活的熔断器并行发起恢复探测：
        // 保证每个到期实例都真正收到一次下游探测，多实例同时到期也互不阻塞。
        scheduler.start(DEMO_SERVICE, healthConfig.probeIntervalMillis(),
                () -> gatewayForwarder.sweepRecoveryProbes(DEMO_SERVICE));
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
