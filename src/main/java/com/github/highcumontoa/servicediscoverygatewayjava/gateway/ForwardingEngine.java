package com.github.highcumontoa.servicediscoverygatewayjava.gateway;

import com.github.highcumontoa.servicediscoverygatewayjava.circuit.CircuitBreaker;
import com.github.highcumontoa.servicediscoverygatewayjava.circuit.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.downstream.DownstreamTransport;
import com.github.highcumontoa.servicediscoverygatewayjava.idempotency.IdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.loadbalancer.LoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.model.AttemptFailure;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DispatchRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DispatchResult;
import com.github.highcumontoa.servicediscoverygatewayjava.model.DownstreamResponse;
import com.github.highcumontoa.servicediscoverygatewayjava.model.FailureType;
import com.github.highcumontoa.servicediscoverygatewayjava.model.GatewayException;
import com.github.highcumontoa.servicediscoverygatewayjava.model.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.registry.ServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * 转发引擎：健康快照选实例 -> 熔断判定 -> 调用下游 -> 失败分类 -> 受限度重试。
 *
 * 关键约束：
 * - 仅从注册中心的健康快照选择；已摘除/已注销实例绝不会被选中；
 * - 熔断 OPEN 快速失败、不调用下游；OPEN 到期后只放一个 HALF_OPEN 探测；
 * - 不可安全重试请求失败后抛出原失败类型，绝不静默重试；
 * - 可重试请求最多尝试 maxAttempts 次、退避后换实例再试，不向同一实例放大压力；
 * - 相同 idempotencyKey 的并发请求共享同一次执行。
 * 每次选择与判定均打印输入、选择结果与依据。
 */
public class ForwardingEngine {

    private static final Logger log = LoggerFactory.getLogger(ForwardingEngine.class);

    private final ServiceRegistry registry;
    private final DownstreamTransport transport;
    private final LoadBalancer loadBalancer;
    private final RetryPolicy retryPolicy;
    private final IdempotencyDeduplicator deduplicator;
    private final long timeoutMillis;

    private final ThreadLocal<List<AttemptFailure>> lastFailures = ThreadLocal.withInitial(ArrayList::new);

    public ForwardingEngine(ServiceRegistry registry, DownstreamTransport transport, LoadBalancer loadBalancer,
                            RetryPolicy retryPolicy, IdempotencyDeduplicator deduplicator,
                            long timeoutMillis) {
        this.registry = registry;
        this.transport = transport;
        this.loadBalancer = loadBalancer;
        this.retryPolicy = retryPolicy;
        this.deduplicator = deduplicator;
        this.timeoutMillis = timeoutMillis;
    }

    public DispatchResult forward(DispatchRequest request) {
        log.info("[forward] 输入 service={} key={} safeRetry={} payload={}",
                request.serviceName(), request.idempotencyKey(), request.safeRetry(), request.payload());
        String key = request.idempotencyKey();
        if (key == null || key.isBlank()) {
            return doForward(request);
        }
        // 幂等去重：首个请求执行，并发重复请求等待复用结果
        CompletableFuture<Object> execution = new CompletableFuture<>();
        Optional<CompletableFuture<Object>> inflight = deduplicator.tryAcquire(key, execution);
        if (inflight.isPresent()) {
            try {
                Object shared = inflight.get().join();
                DispatchResult original = (DispatchResult) shared;
                DispatchResult dup = new DispatchResult(original.instanceId(), original.statusCode(),
                        original.body(), original.attempts(), true);
                log.info("[forward] key={} 复用在途结果 instance={} duplicated=true", key, dup.instanceId());
                return dup;
            } finally {
                lastFailures.get().clear();
            }
        }
        try {
            DispatchResult result = doForward(request);
            execution.complete(result);
            return result;
        } catch (RuntimeException e) {
            execution.completeExceptionally(e);
            throw e;
        } finally {
            deduplicator.release(key, execution);
        }
    }

    private DispatchResult doForward(DispatchRequest request) {
        List<AttemptFailure> failures = lastFailures.get();
        failures.clear();
        Set<String> tried = new HashSet<>();
        GatewayException lastDownstreamError = null;
        int downstreamAttempts = 0;

        while (true) {
            List<ServiceInstance> healthy = registry.healthyInstances(request.serviceName());
            loadBalancer.onHealthySetChanged(healthy);
            ServiceInstance chosen = loadBalancer.choose(healthy, tried);
            log.info("[forward] 下游尝试={} 健康候选={} 已尝试={} 选择={}",
                    downstreamAttempts, ids(healthy), tried, chosen == null ? "null" : chosen.id());

            if (chosen == null) {
                // 候选耗尽：按“从无实例 / 仅熔断 / 下游失败”给出可区分判定
                if (healthy.isEmpty() && downstreamAttempts == 0 && failures.isEmpty()) {
                    log.info("[forward] 判定：NO_HEALTHY_INSTANCE，快速失败");
                    throw new GatewayException(FailureType.NO_HEALTHY_INSTANCE, "无健康实例");
                }
                FailureType onlyType = failures.isEmpty() ? null : failures.get(0).failureType();
                boolean allCircuitOpen = onlyType == FailureType.CIRCUIT_OPEN
                        && failures.stream().allMatch(f -> f.failureType() == FailureType.CIRCUIT_OPEN);
                if (allCircuitOpen) {
                    log.info("[forward] 判定：CIRCUIT_OPEN，全部候选熔断，快速失败");
                    throw new GatewayException(FailureType.CIRCUIT_OPEN, "无可用实例：熔断打开");
                }
                throw exhausted(lastDownstreamError);
            }
            tried.add(chosen.id());

            CircuitBreaker breaker = registry.circuitBreaker(request.serviceName(), chosen.id());
            if (breaker == null || !breaker.allowRequest()) {
                CircuitState state = breaker == null ? null : breaker.state();
                failures.add(new AttemptFailure(downstreamAttempts + 1, chosen.id(),
                        FailureType.CIRCUIT_OPEN, "熔断器状态=" + state + "，快速失败不调用下游"));
                log.info("[forward] 实例 {} 判定：CIRCUIT_OPEN({})，换路尝试其它实例（不消耗下游尝试次数）",
                        chosen.id(), state);
                // 请求未到达下游，换路不是重试、不放大压力；tried 集合保证不会无限循环
                continue;
            }

            downstreamAttempts++;
            try {
                DownstreamResponse response = transport.invoke(chosen, request.payload(), timeoutMillis);
                if (response.statusCode() >= 500) {
                    throw new GatewayException(FailureType.DOWNSTREAM_ERROR,
                            "下游返回错误状态码 " + response.statusCode());
                }
                breaker.recordSuccess();
                DispatchResult result = new DispatchResult(chosen.id(), response.statusCode(),
                        response.body(), downstreamAttempts, false);
                log.info("[forward] 成功 instance={} status={} 下游尝试={} 依据=响应<500，记录熔断成功",
                        chosen.id(), response.statusCode(), downstreamAttempts);
                return result;
            } catch (GatewayException e) {
                breaker.recordFailure();
                failures.add(new AttemptFailure(downstreamAttempts, chosen.id(),
                        e.getFailureType(), e.getMessage()));
                lastDownstreamError = e;
                log.info("[forward] 实例 {} 失败类型={} 熔断记录后状态={}",
                        chosen.id(), e.getFailureType(), breaker.state());

                if (!request.safeRetry()) {
                    log.info("[forward] 判定：NON_RETRYABLE_REQUEST，不做重试，原始类型={}",
                            e.getFailureType());
                    throw new GatewayException(FailureType.NON_RETRYABLE_REQUEST,
                            "不可安全重试请求，失败类型=" + e.getFailureType() + "，不做重试", e);
                }
                if (retryPolicy.canRetry(true, e.getFailureType(), downstreamAttempts)) {
                    log.info("[forward] 可重试且未达上限 {}，退避 {}ms 后换实例",
                            retryPolicy.maxAttempts(), retryPolicy.backoffMillis());
                    backoff();
                    continue;
                }
                // 达到尝试次数上限：保留原始失败类型，便于区分超时/拒绝/下游错误
                if (downstreamAttempts >= retryPolicy.maxAttempts()) {
                    log.info("[forward] 判定：已达最大尝试次数 {}，保留原始失败类型 {}",
                            retryPolicy.maxAttempts(), e.getFailureType());
                    throw e;
                }
                // 仍有重试额度但没有可换的健康实例
                throw exhausted(e);
            }
        }
    }

    private GatewayException exhausted(GatewayException cause) {
        return new GatewayException(FailureType.RETRY_EXHAUSTED,
                "已达最大尝试次数 " + retryPolicy.maxAttempts()
                        + "，最后失败=" + (cause == null ? "unknown" : cause.getFailureType()), cause);
    }

    private void backoff() {
        if (retryPolicy.backoffMillis() <= 0) {
            return;
        }
        try {
            Thread.sleep(retryPolicy.backoffMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static List<String> ids(List<ServiceInstance> instances) {
        return instances.stream().map(ServiceInstance::id).toList();
    }

    /** 当前线程最近一次转发的失败轨迹（测试断言用）。 */
    public List<AttemptFailure> lastFailures() {
        return List.copyOf(lastFailures.get());
    }
}
