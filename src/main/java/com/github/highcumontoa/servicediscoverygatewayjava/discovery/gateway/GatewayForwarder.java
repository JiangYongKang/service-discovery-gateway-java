package com.github.highcumontoa.servicediscoverygatewayjava.discovery.gateway;

import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreaker;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.CircuitBreakerConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.circuit.DefaultCircuitBreaker;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.downstream.DownstreamHandler;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceInstance;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.instance.ServiceRegistry;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.loadbalancer.LoadBalancer;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.CircuitState;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamException;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.DownstreamResponse;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.FailureType;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardOutcome;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ForwardRequest;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.IdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.InMemoryIdempotencyDeduplicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 容错请求转发器。
 *
 * 一次转发的完整判定链（每一步都打印输入/选择结果/判定依据）：
 * 1. 幂等去重：同键并发请求只产生一次真实执行链（含其内部重试）；
 * 2. 每轮都从注册中心取最新健康快照——刚摘除/注销的实例本轮即不可见；
 * 3. 负载均衡在“健康且熔断放行且本次未试过”的候选中选择，重试排除已试实例；
 * 4. 实例级熔断器 OPEN 时直接跳过（CIRCUIT_OPEN），不向下游放大流量；
 * 5. 执行并区分失败类型：TIMEOUT / CONNECTION_REFUSED / DOWNSTREAM_ERROR；
 * 6. 仅可安全重试、且失败属传输类、且未超 maxAttempts、且还有别的健康实例时才退避重试；
 *    非可重试请求绝不静默重试（只执行一次）；
 * 7. 熔断统计只计入“真实执行过”的尝试；熔断打开本身不计为下游失败。
 */
public class GatewayForwarder {

    private static final Logger log = LoggerFactory.getLogger(GatewayForwarder.class);

    private final ServiceRegistry registry;
    private final LoadBalancer loadBalancer;
    private final DownstreamHandler downstream;
    private final RetryPolicy retryPolicy;
    private final CircuitBreakerConfig circuitConfig;
    private final LongSupplier clock;
    private final long requestTimeoutMillis;
    private final Sleeper sleeper;
    private final IdempotencyDeduplicator<ForwardOutcome> deduplicator =
            new InMemoryIdempotencyDeduplicator<>();

    private final Map<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    public GatewayForwarder(ServiceRegistry registry,
                            LoadBalancer loadBalancer,
                            DownstreamHandler downstream,
                            RetryPolicy retryPolicy,
                            CircuitBreakerConfig circuitConfig,
                            LongSupplier clock) {
        this(registry, loadBalancer, downstream, retryPolicy, circuitConfig,
                clock, registry.config().probeTimeoutMillis(), millis -> {
                    try {
                        Thread.sleep(millis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
    }

    /** 测试接缝：可注入时钟、请求超时与退避等待（测试中零等待）。 */
    public GatewayForwarder(ServiceRegistry registry,
                     LoadBalancer loadBalancer,
                     DownstreamHandler downstream,
                     RetryPolicy retryPolicy,
                     CircuitBreakerConfig circuitConfig,
                     LongSupplier clock,
                     long requestTimeoutMillis,
                     Sleeper sleeper) {
        this.registry = registry;
        this.loadBalancer = loadBalancer;
        this.downstream = downstream;
        this.retryPolicy = retryPolicy;
        this.circuitConfig = circuitConfig;
        this.clock = clock;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.sleeper = sleeper;
    }

    public ForwardOutcome forward(ForwardRequest request) {
        log.info("forward input: service={}, idempotencyKey={}, retryable={}, payload={}",
                request.serviceName(), request.idempotencyKey(), request.retryable(), request.payload());
        try {
            ForwardOutcome outcome = deduplicator.execute(request.idempotencyKey(),
                    () -> doForward(request));
            if (outcome.success()) {
                log.info("forward outcome: service={} -> SUCCESS via instances={}",
                        request.serviceName(), attemptIds(outcome));
            } else {
                log.warn("forward outcome: service={} -> FAILED type={}, reason={}",
                        request.serviceName(), outcome.failureType(), outcome.reason());
            }
            return outcome;
        } catch (ForwardFailedException failed) {
            log.warn("forward outcome: service={} -> FAILED type={}, reason={}",
                    request.serviceName(), failed.outcome.failureType(), failed.outcome.reason());
            return failed.outcome;
        }
    }

    private static List<String> attemptIds(ForwardOutcome outcome) {
        return outcome.attempts().stream().map(ForwardOutcome.AttemptRecord::instanceId).toList();
    }

    private ForwardOutcome doForward(ForwardRequest request) {
        List<ForwardOutcome.AttemptRecord> attempts = new ArrayList<>();
        Set<String> triedInstances = new LinkedHashSet<>();
        int maxAttempts = request.retryable() ? retryPolicy.maxAttempts() : 1;
        FailureType lastFailureType = null;
        String lastFailureDetail = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            long now = clock.getAsLong();
            List<ServiceInstance> healthy = registry.healthyInstances(request.serviceName());
            // 在最新健康快照上剔除熔断未放行的实例
            List<ServiceInstance> passable = new ArrayList<>();
            for (ServiceInstance instance : healthy) {
                CircuitBreaker breaker = breakerFor(instance.instanceId());
                if (breaker.allowRequest(now)) {
                    passable.add(instance);
                } else {
                    log.info("skip instance={}: circuit={} (CIRCUIT_OPEN, fail-fast, no downstream pressure)",
                            instance.instanceId(), breaker.state());
                }
            }
            ServiceInstance chosen = loadBalancer.choose(request.serviceName(), passable, triedInstances);
            if (chosen == null) {
                FailureType type = !healthy.isEmpty() && passable.isEmpty()
                        ? FailureType.CIRCUIT_OPEN : FailureType.NO_HEALTHY_INSTANCE;
                String reason = "no candidate on attempt " + attempt + "/" + maxAttempts
                        + " (healthy=" + healthy.size() + ", passableCircuits=" + passable.size()
                        + ", triedThisRequest=" + triedInstances + ")";
                return fail(type, reason, attempts);
            }

            triedInstances.add(chosen.instanceId());
            CircuitBreaker chosenBreaker = breakerFor(chosen.instanceId());
            AttemptResult result = invokeOnce(chosen, request, attempt, attempts);

            switch (result.kind()) {
                case SUCCESS -> {
                    chosenBreaker.recordSuccess(clock.getAsLong());
                    return new ForwardOutcome(true, result.response(), null,
                            "succeeded on attempt " + attempt, List.copyOf(attempts));
                }
                case DOWNSTREAM_5XX -> {
                    chosenBreaker.recordFailure(clock.getAsLong());
                    lastFailureType = FailureType.DOWNSTREAM_ERROR;
                    lastFailureDetail = "instance=" + chosen.instanceId()
                            + " returned " + result.response().statusCode();
                }
                case TRANSPORT_FAILURE -> {
                    chosenBreaker.recordFailure(clock.getAsLong());
                    lastFailureType = result.failureType();
                    lastFailureDetail = "instance=" + chosen.instanceId() + " -> " + result.failureType();
                }
            }

            // 判定顺序：不可重试 -> 非传输类失败 -> 次数耗尽（统一归并 RETRY_EXHAUSTED，
            // 明细保留真实失败类型）-> 没有别的可用实例（避免无意义重试放大）
            if (!request.retryable()) {
                return fail(lastFailureType,
                        giveUpReason(request, attempt, maxAttempts, result, lastFailureDetail, triedInstances),
                        attempts);
            }
            if (result.kind() != AttemptKind.TRANSPORT_FAILURE
                    || (result.failureType() != FailureType.TIMEOUT
                    && result.failureType() != FailureType.CONNECTION_REFUSED)) {
                return fail(lastFailureType,
                        giveUpReason(request, attempt, maxAttempts, result, lastFailureDetail, triedInstances),
                        attempts);
            }
            if (attempt >= maxAttempts) {
                return fail(FailureType.RETRY_EXHAUSTED,
                        "retry budget exhausted after " + maxAttempts + " attempts; lastFailure="
                                + lastFailureType + " (" + lastFailureDetail + ")", attempts);
            }
            if (!hasOtherPassableInstance(request, triedInstances)) {
                return fail(lastFailureType,
                        giveUpReason(request, attempt, maxAttempts, result, lastFailureDetail, triedInstances),
                        attempts);
            }
            long backoff = retryPolicy.backoffMillis() * attempt;
            log.info("retry decision: failure={} is transport-retriable, attempt {}/{} used, "
                            + "backoff={}ms (linear), tried={}",
                    lastFailureType, attempt, maxAttempts, backoff, triedInstances);
            if (backoff > 0) {
                sleeper.sleep(backoff);
            }
        }

        return fail(FailureType.RETRY_EXHAUSTED,
                "retries exhausted after " + maxAttempts + " attempts; lastFailure="
                        + lastFailureType + " (" + lastFailureDetail + ")", attempts);
    }

    private AttemptResult invokeOnce(ServiceInstance chosen, ForwardRequest request,
                                     int attempt, List<ForwardOutcome.AttemptRecord> attempts) {
        String id = chosen.instanceId();
        try {
            DownstreamResponse response = downstream.invoke(id, request.payload(), requestTimeoutMillis);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                attempts.add(new ForwardOutcome.AttemptRecord(id, attempt, "SUCCESS(" + response.statusCode() + ")"));
                log.info("attempt {}: choose={} -> SUCCESS status={}", attempt, id, response.statusCode());
                return AttemptResult.success(response);
            }
            attempts.add(new ForwardOutcome.AttemptRecord(id, attempt,
                    "DOWNSTREAM_ERROR(" + response.statusCode() + ")"));
            log.info("attempt {}: choose={} -> DOWNSTREAM_ERROR status={}", attempt, id, response.statusCode());
            return AttemptResult.downstream5xx(response);
        } catch (DownstreamException e) {
            attempts.add(new ForwardOutcome.AttemptRecord(id, attempt, String.valueOf(e.failureType())));
            log.warn("attempt {}: choose={} -> {}", attempt, id, e.failureType());
            return AttemptResult.transportFailure(e.failureType());
        }
    }

    private boolean hasOtherPassableInstance(ForwardRequest request, Set<String> triedInstances) {
        long now = clock.getAsLong();
        for (ServiceInstance instance : registry.healthyInstances(request.serviceName())) {
            if (triedInstances.contains(instance.instanceId())) {
                continue;
            }
            if (breakerFor(instance.instanceId()).allowRequest(now)) {
                return true;
            }
        }
        return false;
    }

    private String giveUpReason(ForwardRequest request, int attempt, int maxAttempts,
                                AttemptResult result, String detail, Set<String> triedInstances) {
        if (!request.retryable()) {
            return "non-retriable request: exactly 1 attempt, no silent retry (" + detail + ")";
        }
        if (result.kind() == AttemptKind.DOWNSTREAM_5XX) {
            return "downstream 5xx is not auto-retried to avoid duplicating an executed request ("
                    + detail + ")";
        }
        if (attempt >= maxAttempts) {
            return "max attempts " + maxAttempts + " reached (" + detail + ")";
        }
        return "no other healthy/circuit-closed instance left for retry; tried="
                + triedInstances + " (" + detail + ")";
    }

    private static ForwardOutcome fail(FailureType type, String reason,
                                       List<ForwardOutcome.AttemptRecord> attempts) {
        throw new ForwardFailedException(
                new ForwardOutcome(false, null, type, reason, List.copyOf(attempts)));
    }

    private CircuitBreaker breakerFor(String instanceId) {
        return breakers.computeIfAbsent(instanceId,
                id -> new DefaultCircuitBreaker(id, circuitConfig));
    }

    public CircuitStateView circuitView(String serviceName, String instanceId) {
        CircuitBreaker breaker = breakers.get(instanceId);
        return new CircuitStateView(instanceId,
                breaker == null ? CircuitState.CLOSED.name() : breaker.state().name());
    }

    public void forceOpen(String serviceName, String instanceId) {
        ((DefaultCircuitBreaker) breakerFor(instanceId)).forceOpen(clock.getAsLong());
    }

    public record CircuitStateView(String instanceId, String state) {
    }

    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis);
    }

    private enum AttemptKind {
        SUCCESS, DOWNSTREAM_5XX, TRANSPORT_FAILURE
    }

    private record AttemptResult(AttemptKind kind,
                                 DownstreamResponse response,
                                 FailureType failureType) {
        static AttemptResult success(DownstreamResponse response) {
            return new AttemptResult(AttemptKind.SUCCESS, response, null);
        }

        static AttemptResult downstream5xx(DownstreamResponse response) {
            return new AttemptResult(AttemptKind.DOWNSTREAM_5XX, response, null);
        }

        static AttemptResult transportFailure(FailureType type) {
            return new AttemptResult(AttemptKind.TRANSPORT_FAILURE, null, type);
        }
    }

    /** 失败结果以异常穿透去重 future，由 forward 统一回收为 ForwardOutcome。 */
    static final class ForwardFailedException extends RuntimeException {
        private final ForwardOutcome outcome;

        ForwardFailedException(ForwardOutcome outcome) {
            super(outcome.reason(), null, false, false);
            this.outcome = outcome;
        }
    }

}
