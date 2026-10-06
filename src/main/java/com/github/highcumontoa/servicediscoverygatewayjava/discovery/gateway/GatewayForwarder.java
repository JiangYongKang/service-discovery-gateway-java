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
import java.util.concurrent.Executor;
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
 *    冷却到期（或半开探测失活）由后台周期恢复扫描或本轮真实请求通过
 *    tryAcquireRecoveryProbe 原子领取唯一探测——领取与权重无关，冷却到点的实例
 *    （含低权重实例）一定真实收到一次下游探测；多实例同时到期时各自独立并行探测；
 * 5. 负载均衡在“健康且有余量且熔断放行且本次未试过”的候选中按权重分摊；
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
    /**
     * 后台恢复探测执行器：每个到期实例的探测提交为独立任务，多实例同时到期时并行探测、
     * 互不阻塞。生产装配为守护线程池；测试可注入同步 Executor 做确定性断言。
     */
    private final Executor probeExecutor;

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

    /** 测试接缝：在重试预算之外，额外注入同步执行器（恢复探测在调用线程内完成，结果确定）。 */
    public GatewayForwarder(ServiceRegistry registry,
                            LoadBalancer loadBalancer,
                            DownstreamHandler downstream,
                            RetryPolicy retryPolicy,
                            CircuitBreakerConfig circuitConfig,
                            LongSupplier clock,
                            long requestTimeoutMillis,
                            Sleeper sleeper,
                            RetryBudget retryBudget,
                            Executor probeExecutor) {
        this.registry = registry;
        this.loadBalancer = loadBalancer;
        this.downstream = downstream;
        this.retryPolicy = retryPolicy;
        this.circuitConfig = circuitConfig;
        this.clock = clock;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.sleeper = sleeper;
        this.retryBudget = retryBudget;
        this.probeExecutor = probeExecutor;
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
        this(registry, loadBalancer, downstream, retryPolicy, circuitConfig,
                clock, requestTimeoutMillis, sleeper, retryBudget,
                java.util.concurrent.Executors.newFixedThreadPool(8, r -> {
                    Thread t = new Thread(r, "circuit-recovery-probe");
                    t.setDaemon(true);
                    return t;
                }));
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

            // 在有余量的快照上做熔断门控：
            // - CLOSED 实例正常放行进入加权候选；
            // - 冷却到期/探测失活的 OPEN/HALF_OPEN 实例记为“探测到期”，本轮可领取一次
            //   真实探测，领取成功即成为本轮唯一探测目标；
            // - 熔断未放行且未到期的实例快速失败跳过（CIRCUIT_OPEN），不放大下游流量。
            // 注意门控本身不翻牌，翻牌只发生在真正领取探测时（tryAcquireRecoveryProbe），
            // 因此读状态/容量满的实例不会把熔断器带进“卡在 HALF_OPEN”的状态。
            List<ServiceInstance> passable = new ArrayList<>();
            List<ServiceInstance> probeDue = new ArrayList<>();
            for (ServiceInstance instance : withCapacity) {
                CircuitBreaker breaker = breakerFor(instance.instanceId());
                if (breaker.state() == CircuitState.CLOSED) {
                    passable.add(instance);
                } else if (breaker.isRecoveryProbeDue(now)) {
                    probeDue.add(instance);
                } else {
                    circuitOpenSkips.computeIfAbsent(instance.instanceId(),
                            k -> new AtomicLong()).incrementAndGet();
                    log.info("skip instance={}: circuit={} (CIRCUIT_OPEN, fail-fast, no downstream pressure)",
                            instance.instanceId(), breaker.state());
                }
            }

            ServiceInstance chosen = selectInstance(request, passable, probeDue, triedInstances, now);
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
     * - 冷却到期（或半开占坑失活）的实例优先：对它们逐个原子领取恢复探测，
     *   领取成功者成为本轮<strong>唯一</strong>探测目标——领取与权重无关，
     *   低权重实例冷却到点也一定会被真实探测；同实例并发只有一个领取能成功；
     * - 没有需要探测的实例时，其余 CLOSED 实例走负载均衡按权重分摊；
     * - 选中后 tryAcquireInFlight 失败（名额被并发抢走）则本轮排除该实例重选，
     *   名额计数绝不超发。
     */
    private ServiceInstance selectInstance(ForwardRequest request, List<ServiceInstance> passable,
                                           List<ServiceInstance> probeDue, Set<String> triedInstances,
                                           long now) {
        Set<String> excluded = new HashSet<>(triedInstances);
        for (ServiceInstance candidate : probeDue) {
            if (excluded.contains(candidate.instanceId())) {
                continue;
            }
            CircuitBreaker breaker = breakerFor(candidate.instanceId());
            // 先占名额再领取：占不到名额不领取探测，避免占着半开坑却发不出请求
            if (!candidate.tryAcquireInFlight()) {
                excluded.add(candidate.instanceId());
                capacityFullSkips.computeIfAbsent(candidate.instanceId(),
                        k -> new AtomicLong()).incrementAndGet();
                log.info("skip instance={}: CAPACITY_FULL on probe acquire (raced), "
                        + "probe deferred to next round", candidate.instanceId());
                continue;
            }
            if (!breaker.tryAcquireRecoveryProbe(now)) {
                // 并发窗口里探测已被别的线程/后台扫描领取：归还名额，按普通候选处理
                candidate.releaseInFlight();
                if (breaker.state() == CircuitState.CLOSED) {
                    passable.add(candidate);
                }
                continue;
            }
            log.info("probe priority: instance={} recovery probe claimed at cooldown/stale deadline, "
                            + "selected as the single trial request (inFlight={}/{})",
                    candidate.instanceId(), candidate.inFlight(),
                    candidate.maxConcurrency() <= 0 ? "unlimited" : candidate.maxConcurrency());
            return candidate;
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
            CircuitBreaker breaker = breakerFor(instance.instanceId());
            if (breaker.state() == CircuitState.CLOSED || breaker.isRecoveryProbeDue(now)) {
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
                id -> new DefaultCircuitBreaker(id, circuitConfig, clock));
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
     * 一轮“熔断恢复探测”扫描：对该服务中<strong>每个</strong>冷却到点（或半开占坑失活）
     * 的实例各自发起一次真实下游探测。
     *
     * 语义保证：
     * - 各实例独立：为每个到期实例提交一个独立探测任务，多实例同时到期时并行探测、
     *   互不阻塞——不会只恢复其中一个、其余永久停在 HALF_OPEN；
     * - 单实例单飞：领取动作 {@link CircuitBreaker#tryAcquireRecoveryProbe} 是原子的，
     *   与转发流量并发时同一实例同一探测窗口内也恰好只有一个探测，不会一拥而上；
     * - 真实探测：探测是一次真正的下游调用；成功立即 CLOSED 重新接流量，
     *   失败立即回 OPEN 并以当前时刻重新计时，新冷却期内不再探测；
     * - 不占用业务在途名额：恢复探测是基础设施行为，避免把实例判成满载而推迟恢复。
     *
     * @return 本轮真正领取并发出探测的实例数
     */
    public int sweepRecoveryProbes(String serviceName) {
        long now = clock.getAsLong();
        ServiceSnapshot snapshot = registry.snapshot(serviceName);
        List<String> ids = new ArrayList<>(snapshot.healthyIds());
        ids.addAll(snapshot.unhealthyIds());
        int dispatched = 0;
        for (String id : ids) {
            CircuitBreaker breaker = breakers.get(id);
            if (breaker == null) {
                // 从未在转发中出现过的实例没有熔断器，无需恢复探测
                continue;
            }
            // 原子领取：领取失败说明已在探测窗口内（流量或上一轮已领取），本轮跳过
            if (!breaker.tryAcquireRecoveryProbe(now)) {
                continue;
            }
            dispatched++;
            probeExecutor.execute(() -> runRecoveryProbe(serviceName, id));
        }
        if (dispatched > 0) {
            log.info("recovery sweep: service={}, dueInstancesProbed={}/{}, "
                            + "each instance probed independently in parallel",
                    serviceName, dispatched, ids.size());
        }
        return dispatched;
    }

    /** 对单个实例执行一次真实恢复探测并落地结果（成功恢复 / 失败重新计时）。 */
    private void runRecoveryProbe(String serviceName, String instanceId) {
        CircuitBreaker breaker = breakerFor(instanceId);
        try {
            DownstreamResponse response =
                    downstream.invoke(instanceId, "circuit-recovery-probe", requestTimeoutMillis);
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                breaker.recordSuccess(clock.getAsLong());
                log.info("recovery probe result: instance={} -> UP status={}; "
                        + "decision=CLOSED, traffic restored immediately", instanceId, response.statusCode());
            } else {
                breaker.recordFailure(clock.getAsLong(), FailureType.DOWNSTREAM_ERROR);
                log.warn("recovery probe result: instance={} -> DOWNSTREAM_ERROR status={}; "
                        + "decision=OPEN, cooldown restarted", instanceId, response.statusCode());
            }
        } catch (DownstreamException e) {
            breaker.recordFailure(clock.getAsLong(), e.failureType());
            log.warn("recovery probe result: instance={} -> {}; decision=OPEN, cooldown restarted "
                    + "(no probe again until new cooldown elapses)", instanceId, e.failureType());
        } catch (RuntimeException e) {
            // 兜底：探测本身异常也必须落地为失败，不能让实例卡在 HALF_OPEN
            breaker.recordFailure(clock.getAsLong(), FailureType.DOWNSTREAM_ERROR);
            log.error("recovery probe result: instance={} -> unexpected error; "
                    + "decision=OPEN, cooldown restarted", instanceId, e);
        }
    }

    /**
     * 只读运行状态视图：每个实例的熔断状态、是否正在半开探测、最近失败类型分布、
     * 被熔断/满载跳过的累计次数与当前在途名额占用。
     *
     * 只读取快照与计数器，不调用任何会改变状态的方法（如 allowRequest），
     * 查看视图本身不影响熔断恢复与流量分发。
     */
    public List<InstanceStatusView> statusView(String serviceName) {
        long now = clock.getAsLong();
        ServiceSnapshot snapshot = registry.snapshot(serviceName);
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
