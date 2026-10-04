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
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ServiceSnapshot;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.IdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.InMemoryIdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.RetryBudget;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.RetryBudgetConfig;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.SlidingWindowRetryBudget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
    private final RetryBudget retryBudget;
    private final CircuitBreakerConfig circuitConfig;
    private final LongSupplier clock;
    private final long requestTimeoutMillis;
    private final Sleeper sleeper;
    private final IdempotencyDeduplicator<ForwardOutcome> deduplicator =
            new InMemoryIdempotencyDeduplicator<>();

    private final Map<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    /** 每实例被熔断跳过的累计次数（只读视图用，仅增不减）。 */
    private final Map<String, AtomicLong> circuitSkips = new ConcurrentHashMap<>();
    /** 每实例被满载跳过的累计次数（只读视图用，仅增不减）。 */
    private final Map<String, AtomicLong> capacitySkips = new ConcurrentHashMap<>();

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
        this(registry, loadBalancer, downstream, retryPolicy, circuitConfig, clock,
                requestTimeoutMillis, sleeper,
                new SlidingWindowRetryBudget(RetryBudgetConfig.disabled()));
    }

    /** 完整构造：额外注入按服务的重试预算（默认 disabled 时行为与既有版本一致）。 */
    public GatewayForwarder(ServiceRegistry registry,
                     LoadBalancer loadBalancer,
                     DownstreamHandler downstream,
                     RetryPolicy retryPolicy,
                     CircuitBreakerConfig circuitConfig,
                     LongSupplier clock,
                     long requestTimeoutMillis,
                     Sleeper sleeper,
                     RetryBudget retryBudget) {
        this.registry = registry;
        this.loadBalancer = loadBalancer;
        this.downstream = downstream;
        this.retryPolicy = retryPolicy;
        this.circuitConfig = circuitConfig;
        this.clock = clock;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.sleeper = sleeper;
        this.retryBudget = retryBudget;
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
            // 熔断恢复探测：冷却到期的实例由本请求原子获取唯一探测权，
            // 本轮必须真实向它发一次下游探测（并发请求只能有一个拿到探测权）
            ServiceInstance probePinned = null;
            for (ServiceInstance instance : healthy) {
                if (breakerFor(instance.instanceId()).tryAcquireProbe(now)) {
                    probePinned = instance;
                    log.info("probe pinned: instance={} cooldown elapsed, this request carries the "
                                    + "single half-open probe (concurrent requests keep failing fast)",
                            instance.instanceId());
                    break;
                }
            }
            // 在最新健康快照上剔除熔断未放行的实例
            List<ServiceInstance> passable = new ArrayList<>();
            for (ServiceInstance instance : healthy) {
                if (instance == probePinned) {
                    continue; // 探测实例走 pinned 通道，不参与常规筛选
                }
                CircuitBreaker breaker = breakerFor(instance.instanceId());
                if (breaker.allowRequest(now)) {
                    passable.add(instance);
                } else {
                    circuitSkips.computeIfAbsent(instance.instanceId(),
                            k -> new AtomicLong()).incrementAndGet();
                    log.info("skip instance={}: circuit={} (CIRCUIT_OPEN, fail-fast, no downstream pressure)",
                            instance.instanceId(), breaker.state());
                }
            }
            // 容量维度：满载实例本轮不参与分发，其权重份额按比例让给还有余量的实例
            List<ServiceInstance> candidates = new ArrayList<>();
            List<String> fullInstances = new ArrayList<>();
            for (ServiceInstance instance : passable) {
                if (instance.hasCapacity()) {
                    candidates.add(instance);
                } else {
                    fullInstances.add(instance.instanceId());
                    capacitySkips.computeIfAbsent(instance.instanceId(),
                            k -> new AtomicLong()).incrementAndGet();
                    log.info("skip instance={}: at capacity inFlight={}/{} "
                                    + "(CAPACITY_FULL, its weight share is redistributed to instances with headroom)",
                            instance.instanceId(), instance.inFlight(), instance.maxInFlight());
                }
            }
            // 探测实例优先：经过容量检查后本轮必须选中它，保证冷却到期后
            // 真的有一次下游探测请求到达该实例
            ServiceInstance chosen = null;
            Set<String> racedFull = new LinkedHashSet<>();
            if (probePinned != null) {
                if (probePinned.tryAcquireInFlight()) {
                    chosen = probePinned;
                    log.info("probe dispatch: instance={} selected for half-open probe "
                            + "(passed capacity screening)", probePinned.instanceId());
                } else {
                    // 极端情况：探测目标恰好满载（如有未结束的在途长请求），
                    // 归还探测权并重新计时，等下一轮窗口再探
                    breakerFor(probePinned.instanceId()).releaseProbe(clock.getAsLong());
                    capacitySkips.computeIfAbsent(probePinned.instanceId(),
                            k -> new AtomicLong()).incrementAndGet();
                    log.info("probe deferred: instance={} at capacity inFlight={}/{}, "
                                    + "probe released, will retry next window",
                            probePinned.instanceId(), probePinned.inFlight(), probePinned.maxInFlight());
                }
            }
            // 选择 + 原子占名额：选中到占用之间名额可能被并发请求抢走，此时把该实例
            // 临时排除并重选，保证在途计数绝不超限
            while (chosen == null) {
                Set<String> excluded = new LinkedHashSet<>(triedInstances);
                excluded.addAll(racedFull);
                ServiceInstance pick = loadBalancer.choose(request.serviceName(), candidates, excluded);
                if (pick == null) {
                    break;
                }
                if (pick.tryAcquireInFlight()) {
                    chosen = pick;
                    break;
                }
                racedFull.add(pick.instanceId());
                capacitySkips.computeIfAbsent(pick.instanceId(),
                        k -> new AtomicLong()).incrementAndGet();
                log.info("skip instance={}: lost in-flight slot race inFlight={}/{} "
                                + "(CAPACITY_FULL, re-choose among remaining candidates)",
                        pick.instanceId(), pick.inFlight(), pick.maxInFlight());
            }
            if (chosen == null) {
                FailureType type;
                if (healthy.isEmpty()) {
                    type = FailureType.NO_HEALTHY_INSTANCE;
                } else if (passable.isEmpty()) {
                    type = FailureType.CIRCUIT_OPEN;
                } else {
                    type = FailureType.ALL_AT_CAPACITY;
                }
                List<String> atCapacity = new ArrayList<>(fullInstances);
                atCapacity.addAll(racedFull);
                String reason = "no candidate on attempt " + attempt + "/" + maxAttempts
                        + " (healthy=" + healthy.size() + ", passableCircuits=" + passable.size()
                        + ", atCapacity=" + atCapacity
                        + ", triedThisRequest=" + triedInstances + ")";
                return fail(type, reason, attempts);
            }

            triedInstances.add(chosen.instanceId());
            CircuitBreaker chosenBreaker = breakerFor(chosen.instanceId());
            if (attempt == 1) {
                // 首发请求永远照常发出，只计数用于重试占比；预算绝不能拦截首发
                retryBudget.recordFirstAttempt(request.serviceName(), now);
            }
            AttemptResult result;
            try {
                result = invokeOnce(chosen, request, attempt, attempts);
            } finally {
                // 无论成功、超时还是下游报错，名额必须归还，杜绝计数泄漏把实例永久判成满载
                chosen.releaseInFlight();
            }

            switch (result.kind()) {
                case SUCCESS -> {
                    chosenBreaker.recordSuccess(clock.getAsLong());
                    return new ForwardOutcome(true, result.response(), null,
                            "succeeded on attempt " + attempt, List.copyOf(attempts));
                }
                case DOWNSTREAM_5XX -> {
                    chosenBreaker.recordFailure(clock.getAsLong(), FailureType.DOWNSTREAM_ERROR);
                    lastFailureType = FailureType.DOWNSTREAM_ERROR;
                    lastFailureDetail = "instance=" + chosen.instanceId()
                            + " returned " + result.response().statusCode();
                }
                case TRANSPORT_FAILURE -> {
                    chosenBreaker.recordFailure(clock.getAsLong(), result.failureType());
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
            // 按服务的重试预算：故障期间窗口内重试量超限后，首发照常、自动重试被拦，
            // 避免重试风暴把整片下游打垮；窗口滑过后自动恢复
            RetryBudget.Decision budgetDecision =
                    retryBudget.tryConsumeRetry(request.serviceName(), clock.getAsLong());
            if (!budgetDecision.allowed()) {
                return fail(lastFailureType,
                        "auto-retry suppressed by service retry budget (" + budgetDecision.reason()
                                + "); first attempt was executed normally, lastFailure="
                                + lastFailureType + " (" + lastFailureDetail + ")", attempts);
            }
            long backoff = retryPolicy.backoffMillis() * attempt;
            log.info("retry decision: failure={} is transport-retriable, attempt {}/{} used, "
                            + "budget={}, backoff={}ms (linear), tried={}",
                    lastFailureType, attempt, maxAttempts, budgetDecision.reason(), backoff, triedInstances);
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
            if (breakerFor(instance.instanceId()).allowRequest(now) && instance.hasCapacity()) {
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

    /**
     * 只读运行状态视图：每个实例的熔断状态、是否正在半开探测、最近失败类型分布、
     * 被熔断/满载跳过的累计次数，以及权重与在途容量。
     *
     * 本方法只读取既有状态（不创建熔断器、不触发探测权迁移、不修改任何计数），
     * 任意并发查看都不会影响熔断恢复与流量分发本身。
     */
    public RuntimeView runtimeView(String serviceName) {
        ServiceSnapshot snapshot = registry.snapshot(serviceName);
        List<InstanceView> instances = new ArrayList<>();
        List<String> ids = new ArrayList<>(snapshot.healthyIds());
        ids.addAll(snapshot.unhealthyIds());
        for (String id : ids) {
            ServiceInstance instance = registry.getInstance(serviceName, id);
            CircuitBreaker breaker = breakers.get(id); // 只读获取，绝不为查看而创建
            CircuitState state = breaker == null ? CircuitState.CLOSED : breaker.state();
            Map<String, Integer> failures = breaker instanceof DefaultCircuitBreaker dcb
                    ? stringify(dcb.recentFailureDistribution()) : Map.of();
            instances.add(new InstanceView(id,
                    state.name(),
                    state == CircuitState.HALF_OPEN,
                    failures,
                    circuitSkips.getOrDefault(id, new AtomicLong()).get(),
                    capacitySkips.getOrDefault(id, new AtomicLong()).get(),
                    instance == null ? 0 : instance.weight(),
                    instance == null ? 0 : instance.inFlight(),
                    instance == null ? 0 : instance.maxInFlight()));
        }
        return new RuntimeView(serviceName, List.copyOf(instances));
    }

    private static Map<String, Integer> stringify(Map<FailureType, Integer> distribution) {
        Map<String, Integer> result = new LinkedHashMap<>();
        distribution.forEach((type, count) -> result.put(type.name(), count));
        return result;
    }

    public void forceOpen(String serviceName, String instanceId) {
        ((DefaultCircuitBreaker) breakerFor(instanceId)).forceOpen(clock.getAsLong());
    }

    public record CircuitStateView(String instanceId, String state) {
    }

    /** 服务级只读运行视图。 */
    public record RuntimeView(String serviceName, List<InstanceView> instances) {
    }

    /**
     * 单实例只读运行视图。
     *
     * @param probing                   是否正处于半开探测窗口（HALF_OPEN）
     * @param recentFailureDistribution 最近真实失败的类型分布（滑动窗口内计数）
     * @param circuitSkipCount          因熔断被跳过的累计次数
     * @param capacitySkipCount         因满载被跳过的累计次数
     */
    public record InstanceView(String instanceId,
                               String circuitState,
                               boolean probing,
                               Map<String, Integer> recentFailureDistribution,
                               long circuitSkipCount,
                               long capacitySkipCount,
                               int weight,
                               int inFlight,
                               int maxInFlight) {
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
