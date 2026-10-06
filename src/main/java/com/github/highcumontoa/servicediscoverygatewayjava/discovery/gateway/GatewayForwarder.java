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
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryBudgetPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.RetryPolicy;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.model.ServiceSnapshot;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.IdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.InMemoryIdempotencyDeduplicator;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.RetryBudget;
import com.github.highcumontoa.servicediscoverygatewayjava.discovery.retry.SlidingWindowRetryBudget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
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
 * 3. 容量预筛：在途请求已达 maxConcurrency 的实例本轮跳过（CAPACITY_FULL），
 *    其权重份额按比例让给其余有余量实例；名额归还后自动回到分发；
 * 4. 实例级熔断器 OPEN 时直接跳过（CIRCUIT_OPEN），不向下游放大流量；
 *    冷却到点的实例先被记为探测候选（只读判断，不翻牌），选中时才发现子翻牌
 *    （OPEN->HALF_OPEN）并真实发出唯一探测——翻牌与真实探测严格一一对应，
 *    多个实例同时到点时各自独立探测、互不阻塞，不会卡在半开状态；
 * 5. 负载均衡在“健康且有余量且熔断放行且本次未试过”的候选中按权重分摊；
 *    探测候选无视权重优先（低权重实例冷却到点也必然被真实探测）；
 *    选中后以 CAS 原子占用在途名额，请求结束（成功/超时/下游报错）必归还；
 * 6. 执行并区分失败类型：TIMEOUT / CONNECTION_REFUSED / DOWNSTREAM_ERROR；
 * 7. 仅可安全重试、且失败属传输类、且未超 maxAttempts、且还有别的健康实例、
 *    且重试预算（滑动窗口总量/占比）未耗尽时才退避重试；
 *    预算只拦截自动重试，首发请求永远照常发出；
 *    非可重试请求绝不静默重试（只执行一次）；
 * 8. 熔断统计只计入“真实执行过”的尝试；熔断打开本身不计为下游失败。
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
    private final RetryBudget retryBudget;
    private final IdempotencyDeduplicator<ForwardOutcome> deduplicator =
            new InMemoryIdempotencyDeduplicator<>();

    private final Map<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();
    /** 只读统计：各实例因熔断打开被跳过的累计次数。 */
    private final Map<String, AtomicLong> circuitOpenSkips = new ConcurrentHashMap<>();
    /** 只读统计：各实例因满载被跳过的累计次数。 */
    private final Map<String, AtomicLong> capacityFullSkips = new ConcurrentHashMap<>();

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
                new SlidingWindowRetryBudget(RetryBudgetPolicy.disabled()));
    }

    /** 完整接缝：额外注入按服务的重试预算。 */
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
        // 每个逻辑请求只记一次首发（重试预算的占比分母）
        retryBudget.recordRequest(request.serviceName(), clock.getAsLong());

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            long now = clock.getAsLong();
            List<ServiceInstance> healthy = registry.healthyInstances(request.serviceName());

            // 容量预筛（先于熔断判定）：满载实例本轮跳过，且不对它做熔断翻牌，
            // 等它空下来再自动回到分发与探测链路
            List<ServiceInstance> withCapacity = new ArrayList<>();
            for (ServiceInstance instance : healthy) {
                if (instance.hasCapacity()) {
                    withCapacity.add(instance);
                } else {
                    capacityFullSkips.computeIfAbsent(instance.instanceId(),
                            k -> new AtomicLong()).incrementAndGet();
                    log.info("skip instance={}: CAPACITY_FULL (inFlight={}/max={}), "
                                    + "share redistributed to instances with free slots",
                            instance.instanceId(), instance.inFlight(), instance.maxConcurrency());
                }
            }

            // 在有余量的快照上分流：CLOSED 直接放行；冷却到点的 OPEN 实例只记为
            // 探测候选（此处绝不翻牌），其余（OPEN 未到期 / HALF_OPEN 探测中）跳过。
            // 翻牌被推迟到 selectInstance 真正选中该实例时原子发生——这样多个实例
            // 同时冷却到点时，每个实例只会在“确定要向它发探测”的那一刻翻牌，
            // 不会出现翻了牌却没发探测、永远卡在 HALF_OPEN 的实例。
            List<ServiceInstance> passable = new ArrayList<>();
            List<ServiceInstance> probeCandidates = new ArrayList<>();
            for (ServiceInstance instance : withCapacity) {
                CircuitBreaker breaker = breakerFor(instance.instanceId());
                if (breaker.state() == CircuitState.CLOSED) {
                    passable.add(instance);
                } else if (breaker.probeDue(now)) {
                    probeCandidates.add(instance);
                } else {
                    circuitOpenSkips.computeIfAbsent(instance.instanceId(),
                            k -> new AtomicLong()).incrementAndGet();
                    log.info("skip instance={}: circuit={} (CIRCUIT_OPEN, fail-fast, no downstream pressure)",
                            instance.instanceId(), breaker.state());
                }
            }

            ServiceInstance chosen = selectInstance(request, passable, probeCandidates, triedInstances, now);
            if (chosen == null) {
                FailureType type;
                if (healthy.isEmpty()) {
                    type = FailureType.NO_HEALTHY_INSTANCE;
                } else if (withCapacity.isEmpty()) {
                    type = FailureType.CAPACITY_FULL;
                } else if (passable.isEmpty()) {
                    type = FailureType.CIRCUIT_OPEN;
                } else {
                    type = FailureType.CAPACITY_FULL;
                }
                String reason = "no candidate on attempt " + attempt + "/" + maxAttempts
                        + " (healthy=" + healthy.size() + ", withCapacity=" + withCapacity.size()
                        + ", passableCircuits=" + passable.size()
                        + ", probeCandidates=" + probeCandidates.size()
                        + ", triedThisRequest=" + triedInstances + ")";
                return fail(type, reason, attempts);
            }

            triedInstances.add(chosen.instanceId());
            CircuitBreaker chosenBreaker = breakerFor(chosen.instanceId());
            AttemptResult result;
            try {
                result = invokeOnce(chosen, request, attempt, attempts);
            } finally {
                // 无论成功、超时还是下游报错都必须归还名额，杜绝计数泄漏
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
            // -> 重试预算耗尽（只拦自动重试，首发不受影响）
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
            RetryBudget.RetryDecision budgetDecision =
                    retryBudget.tryAcquireRetry(request.serviceName(), clock.getAsLong());
            if (!budgetDecision.allowed()) {
                return fail(lastFailureType,
                        "auto-retry suppressed by retry budget: " + budgetDecision.reason()
                                + " (" + lastFailureDetail + ")", attempts);
            }
            long backoff = retryPolicy.backoffMillis() * attempt;
            log.info("retry decision: failure={} is transport-retriable, attempt {}/{} used, "
                            + "backoff={}ms (linear), tried={}, budget={}",
                    lastFailureType, attempt, maxAttempts, backoff, triedInstances,
                    budgetDecision.reason());
            if (backoff > 0) {
                sleeper.sleep(backoff);
            }
        }

        return fail(FailureType.RETRY_EXHAUSTED,
                "retries exhausted after " + maxAttempts + " attempts; lastFailure="
                        + lastFailureType + " (" + lastFailureDetail + ")", attempts);
    }

    /**
     * 选择本轮实例并以 CAS 原子占用在途名额：
     * - 冷却到点的探测候选优先（无视权重，低权重实例也必然被真实探测）：
     *   先占在途名额，再 tryAcquireProbe 原子翻牌——只有真正拿到探测权的
     *   请求才把实例翻成 HALF_OPEN 并向它发出唯一探测；翻牌被并发请求抢走
     *   时归还名额并看下一个候选，同一实例的探测窗口内永远只有一个探测；
     * - 多个实例同时冷却到点时按候选顺序逐个探测：每个请求只探测一个实例，
     *   后续请求继续探测其余到点实例，各实例独立恢复、互不阻塞；
     * - 其余走负载均衡按权重分摊；
     * - 选中后 tryAcquireInFlight 失败（名额被并发抢走）则本轮排除该实例重选，
     *   名额计数绝不超发。
     */
    private ServiceInstance selectInstance(ForwardRequest request, List<ServiceInstance> passable,
                                           List<ServiceInstance> probeCandidates,
                                           Set<String> triedInstances, long now) {
        Set<String> excluded = new HashSet<>(triedInstances);
        for (ServiceInstance probeCandidate : probeCandidates) {
            if (triedInstances.contains(probeCandidate.instanceId())) {
                continue;
            }
            if (!probeCandidate.tryAcquireInFlight()) {
                capacityFullSkips.computeIfAbsent(probeCandidate.instanceId(),
                        k -> new AtomicLong()).incrementAndGet();
                log.info("skip instance={}: CAPACITY_FULL on probe acquire (raced), try next candidate",
                        probeCandidate.instanceId());
                continue;
            }
            CircuitBreaker breaker = breakerFor(probeCandidate.instanceId());
            if (breaker.tryAcquireProbe(now)) {
                log.info("probe priority: instance={} cooldown elapsed, probe acquired atomically, "
                                + "selected as the single trial request (inFlight={}/{}), weight={} ignored for probes",
                        probeCandidate.instanceId(), probeCandidate.inFlight(),
                        probeCandidate.maxConcurrency() <= 0 ? "unlimited" : probeCandidate.maxConcurrency(),
                        probeCandidate.weight());
                return probeCandidate;
            }
            // 探测权被并发请求拿走（它已成为该实例的唯一探测）：归还名额，本请求走正常分发
            probeCandidate.releaseInFlight();
            log.info("probe race lost: instance={} already being probed by a concurrent request, "
                    + "falling back to normal selection", probeCandidate.instanceId());
        }
        while (true) {
            ServiceInstance candidate =
                    loadBalancer.choose(request.serviceName(), passable, excluded);
            if (candidate == null) {
                return null;
            }
            if (candidate.tryAcquireInFlight()) {
                return candidate;
            }
            // 预筛后名额被并发抢走：本轮排除并重选，它的份额让给其它实例
            excluded.add(candidate.instanceId());
            capacityFullSkips.computeIfAbsent(candidate.instanceId(),
                    k -> new AtomicLong()).incrementAndGet();
            log.info("skip instance={}: CAPACITY_FULL confirmed on acquire "
                            + "(inFlight={}/max={}), re-selecting among remaining candidates",
                    candidate.instanceId(), candidate.inFlight(), candidate.maxConcurrency());
        }
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
            if (!instance.hasCapacity()) {
                continue;
            }
            // 只读判断：CLOSED 或冷却到点即视为可用；此处绝不翻牌，
            // 翻牌只发生在 selectInstance 真正选中探测时
            CircuitBreaker breaker = breakerFor(instance.instanceId());
            if (breaker.state() == CircuitState.CLOSED || breaker.probeDue(now)) {
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

    /**
     * 只读运行状态视图：每个实例的熔断状态、是否正在半开探测、最近失败类型分布、
     * 被熔断/满载跳过的累计次数与当前在途名额占用。
     *
     * 只读取快照与计数器，不调用任何会改变状态的方法（如 allowRequest），
     * 查看视图本身不影响熔断恢复与流量分发。
     */
    public List<InstanceStatusView> statusView(String serviceName) {
        ServiceSnapshot snapshot = registry.snapshot(serviceName);
        long now = clock.getAsLong();
        List<String> ids = new ArrayList<>(snapshot.healthyIds());
        ids.addAll(snapshot.unhealthyIds());
        List<InstanceStatusView> views = new ArrayList<>();
        for (String id : ids) {
            CircuitBreaker breaker = breakers.get(id);
            ServiceInstance instance = registry.getInstance(serviceName, id);
            views.add(new InstanceStatusView(
                    id,
                    breaker == null ? CircuitState.CLOSED.name() : breaker.state().name(),
                    breaker != null && breaker.isProbing(),
                    breaker == null ? Map.of() : breaker.failureTypeDistribution(now),
                    circuitOpenSkips.getOrDefault(id, new AtomicLong()).get(),
                    capacityFullSkips.getOrDefault(id, new AtomicLong()).get(),
                    instance == null ? 1 : instance.weight(),
                    instance == null ? 0 : instance.maxConcurrency(),
                    instance == null ? 0 : instance.inFlight()));
        }
        return views;
    }

    public record CircuitStateView(String instanceId, String state) {
    }

    /** 单实例只读运行状态。 */
    public record InstanceStatusView(String instanceId,
                                     String circuitState,
                                     boolean probing,
                                     Map<FailureType, Integer> failureTypeDistribution,
                                     long circuitOpenSkips,
                                     long capacityFullSkips,
                                     int weight,
                                     int maxConcurrency,
                                     int inFlight) {
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
