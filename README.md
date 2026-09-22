# Service Discovery Gateway（本地可运行的服务发现与容错转发）

用**进程内的多个下游实例**模拟真实集群，实现注册/注销、健康探测与摘除恢复、多实例负载
分发、重试与幂等去重、实例级熔断与半开恢复。所有关键判定都打印日志（输入、选择结果、
判定依据），使实例状态变化与故障链路**可解释、可复现**。

技术栈：Java 21、Spring Boot 3.4、JUnit 5，无外部中间件依赖。

---

## 1. 模块结构

```
discovery/
├── model/          枚举与值对象：InstanceStatus / FailureType / CircuitState /
│                   ForwardRequest / ForwardOutcome / HealthConfig / RetryPolicy ...
├── downstream/     DownstreamHandler（下游调用抽象）+ SimulatedDownstream（进程内模拟器）
├── instance/       ServiceInstance（实例状态机）、ServiceRegistry / InMemoryServiceRegistry
│                   （注册/注销/探测/健康集合）、HealthProbeScheduler（周期探测）
├── loadbalancer/   LoadBalancer + RoundRobinLoadBalancer / RandomLoadBalancer
├── circuit/        CircuitBreaker + DefaultCircuitBreaker（CLOSED/OPEN/HALF_OPEN）
├── retry/          IdempotencyDeduplicator + InMemoryIdempotencyDeduplicator
├── gateway/        GatewayForwarder（发现→熔断→分发→执行→重试→去重 的总装）
├── config/         DiscoveryConfig（Spring Bean 装配与默认参数）
└── web/            DemoController（本地手工验证 HTTP 接口）
```

---

## 2. 实例注册与健康判定规则

### 2.1 注册 / 注销

- `register(service, instanceId)` 幂等：底层用 `ConcurrentHashMap.putIfAbsent`，
  **并发注册同一实例只有一个胜出**，健康集合不会重复计入同一实例。
- `deregister(service, instanceId)` 把实例标记为 `DEREGISTERED` 终态并从注册表移除；
  等待探测锁的注销请求在拿到锁后会二次确认，**注销实例永不复活、永不参与转发**。

### 2.2 健康探测

- 探测由 `HealthProbeScheduler` 单线程定时触发（默认每 **500ms** 一轮），也可通过
  HTTP 手动触发，便于复现。
- 对实例做一次探针调用（超时阈值 `probeTimeoutMillis`，默认 **200ms**）：
  - 成功 → 连续失败计数清零；摘除中的实例恢复 `HEALTHY`，立即重新参与流量；
  - 失败（超时/拒绝/异常）→ 连续失败计数 +1，状态迁移在实例的同步方法内完成，
    **同一实例有细粒度探测锁，并发探测不会重复计数**。
- **摘除条件**：`HEALTHY` 状态下连续失败达到阈值（默认 **3** 次）→ 迁移为
  `UNHEALTHY`，从健康集合消失。
- **摘除冷却**：摘除后 `evictTimeMillis`（默认 **3000ms**）内跳过该实例的探测，
  避免刚摘除就被探测流量反复打扰；冷却结束后才允许做一次复活探测，成功则回归。
- 转发路径**每次都取最新健康快照**，因此摘除/注销在当下一轮即生效，不会把摘除
  实例继续用于转发。

### 2.3 状态机

```
                register                 3 次连续探测失败
  (不存在) ───────────────▶ HEALTHY ───────────────────────▶ UNHEALTHY
                              ▲  ▲  deregister                  │
                  复活探测成功 │  └──────────────┐              │ 注销
                              │                 ▼              ▼
                              └───────────── DEREGISTERED（终态）
```

---

## 3. 分发与重试策略

### 3.1 负载分发

- 支持声明式策略：`ROUND_ROBIN`（默认，每服务独立游标，CAS 推进，对候选数取模）、
  `RANDOM`。
- 选择只依赖“**当前健康快照 ∩ 熔断放行 ∩ 本次未尝试**”的候选集合：
  - 健康集合缩小时取模自然收敛，不会选到已摘除实例；
  - 重试时通过 `excluded` 排除本次已尝试实例，**不把重试压力重新打到故障实例**。

### 3.2 失败类型映射（彼此可区分）

| 现象               | FailureType          | 是否真实到达下游 | 默认是否自动重试 |
|--------------------|----------------------|------------------|------------------|
| 调用超时           | `TIMEOUT`            | 否/不确定        | 是（传输故障）   |
| 连接拒绝           | `CONNECTION_REFUSED` | 否               | 是（传输故障）   |
| 下游返回 4xx/5xx   | `DOWNSTREAM_ERROR`   | 是（业务已执行） | **否**，防重复执行 |
| 无健康实例         | `NO_HEALTHY_INSTANCE`| 否               | 否（快速失败）   |
| 候选全部熔断打开   | `CIRCUIT_OPEN`       | 否               | 否（快速失败）   |
| 可重试请求次数用尽 | `RETRY_EXHAUSTED`    | —                | 终态归并         |

### 3.3 重试规则（防止放大下游压力）

- 只有请求显式声明 `retryable=true` 才允许重试；**不可安全重试的请求只执行一次，
  绝不静默重试**，结果中原样保留真实失败类型与“exactly 1 attempt”的判定依据。
- 最多 `maxAttempts` 次尝试（默认 **3**：1 次正常 + 2 次重试），次数耗尽归并为
  `RETRY_EXHAUSTED`，但每次尝试的实例与结果明细都保留在 `attempts` 中。
- 仅对 `TIMEOUT` / `CONNECTION_REFUSED` 两类传输故障重试；5xx 不自动重试。
- 线性退避 `backoffMillis * attempt`（默认 50ms 起），且仅当“**还有别的健康且
  熔断放行的实例**”时才重试，否则提前终止，不做无意义重试。

### 3.4 幂等去重

- 以请求的 `idempotencyKey` 为键：键为空则不去重。
- 同一键的**首个请求独占执行整条转发链（含其内部重试）**，并发到达的相同键请求
  等待并共享同一个 `CompletableFuture` 结果——**同一幂等标识的并发请求不会造成
  重复执行**。
- 成功结果缓存复用；失败键立即移除（不保留毒缓存），允许后续请求重新发起。

---

## 4. 熔断与恢复条件（实例级）

熔断器为每个下游实例独立维护，状态迁移全部在同步方法内完成：

```
            连续真实调用失败达阈值（默认 3）
  CLOSED ───────────────────────────────────▶ OPEN
    ▲                                           │
    │ 半开探测连续成功 trialSuccessRequired 次   │ 冷却 openMillis（默认 2000ms）到期
    │ （默认 1 次）                              ▼
    └────────────────────────────────────── HALF_OPEN
                  ▲ 半开探测任一失败            │
                  └────────────────────────────┘
```

- **OPEN**：一切请求快速失败（`CIRCUIT_OPEN`），**零下游调用**，不放大故障流量；
- 冷却到期后 `allowRequest` 原子地把状态翻到 `HALF_OPEN`，**同一时刻只放行一个
  探测请求**，其余请求仍快速失败；
- 半开探测成功次数达到 `trialSuccessRequired` → 回 `CLOSED`，流量恢复；
- 半开探测**任一失败 → 立即回 `OPEN` 并重新计时**，必须再等一个冷却周期才会有
  下一次探测。
- 熔断统计只计入“真实执行过”的尝试；被熔断拦截本身不计为下游失败。
- 健康摘除（注册中心视角）与熔断（转发器视角）是两道相互独立的防线，可叠加生效。

---

## 5. 本地验证方法

### 5.1 运行单元测试（25 个，覆盖需求全部关键场景）

```bash
mvn test
```

| 测试类 | 覆盖内容 |
|--------|----------|
| `RegistryConcurrencyTest` | 并发重复注册只生效一次、连续失败摘除、冷却后恢复、注册/注销/探测并发交替的健康集合不变量 |
| `LoadBalancerTest` | 轮询顺序、重试排除已试实例、健康集合缩小时平滑收敛、随机候选过滤 |
| `CircuitBreakerTest` | CLOSED→OPEN 阈值、OPEN 快速失败、半开唯一探测、探测成功恢复、探测失败保持熔断并重新计时 |
| `RetryAndForwardTest` | 传输故障换实例重试、重试次数上限、不可重试只执行一次、TIMEOUT/REFUSED/5xx 区分、无实例/全熔断快速失败 |
| `IdempotencyDedupTest` | 同键 16 并发只执行一次、异键独立执行、失败键不缓存 |
| `GatewayIntegrationTest` | 转发流量驱动熔断→OPEN 零调用→半开恢复；半开失败保持熔断；同键 20 并发仅一次下游执行；健康摘除后流量收敛 |

测试与主流程均通过 SLF4J 打印**输入、选择结果与判定依据**，例如：

```
lb input: service=svc, policy=ROUND_ROBIN, candidates=2, excluded=[i1] -> choose=i3 (basis: ...)
probe result: instance=i1 -> CONNECTION_REFUSED; decision=EVICT(reached 3 consecutive failures, ...)
circuit[i1] OPEN -> HALF_OPEN: elapsed=2000ms >= openMillis=2000ms, exactly ONE trial request allowed
retry decision: failure=CONNECTION_REFUSED is transport-retriable, attempt 1/3 used, backoff=50ms, ...
forward outcome: service=svc -> FAILED type=RETRY_EXHAUSTED, reason=retry budget exhausted ...
```

### 5.2 启动应用并用 HTTP 手工复现

```bash
mvn spring-boot:run          # 或 mvn package && java -jar target/*.jar
```

演示服务统一为 `demo-service`，周期健康探测会自动启动：

```bash
B=http://localhost:8080/demo

# 1) 注册 3 个进程内模拟实例
curl -X POST "$B/register?count=3"

# 2) 正常转发（轮询）
curl -X POST "$B/forward" -H 'Content-Type: application/json' \
  -d '{"payload":"hello","retryable":true}'

# 3) 制造故障：NORMAL / REFUSE / TIMEOUT / ERROR_500
curl -X POST "$B/instances/inst-2/mode?mode=REFUSE"
curl -X POST "$B/instances/inst-3/mode?mode=TIMEOUT"

# 4) 立即触发一轮健康探测（连续 3 轮后 inst-2/inst-3 被摘除；冷却 3 秒）
curl -X POST "$B/probe"

# 5) 注销实例
curl -X POST "$B/instances/inst-1/deregister"

# 6) 不可重试请求（只执行一次，不静默重试）
curl -X POST "$B/forward" -H 'Content-Type: application/json' \
  -d '{"payload":"write","retryable":false}'

# 7) 同一幂等键并发请求（下游只真实执行一次；可并发多次发送观察同一响应）
curl -X POST "$B/forward" -H 'Content-Type: application/json' \
  -d '{"payload":"create-payment","retryable":true,"idempotencyKey":"pay-001"}'

# 8) 查看注册中心、熔断状态与各实例下游调用计数
curl "$B/state"
```

`ForwardOutcome` 的每一次返回都包含 `success / response / failureType / reason /
attempts`（每次尝试的实例、序号、结果），可直接看到选择序列与失败判定依据；
应用日志中同步输出每一步的判定过程。

### 5.3 默认参数（见 `DiscoveryConfig` / `HealthConfig` 等）

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `failureThreshold`（健康） | 3 | 连续探测失败多少次摘除 |
| `evictTimeMillis` | 3000ms | 摘除冷却，到期才允许复活探测 |
| `probeIntervalMillis` | 500ms | 周期探测间隔 |
| `probeTimeoutMillis` | 200ms | 探针调用超时 |
| 熔断 `failureThreshold` | 3 | 连续真实调用失败多少次熔断 |
| `openMillis` | 2000ms | 熔断冷却，到期进入半开探测 |
| `trialSuccessRequired` | 1 | 半开连续成功几次恢复 |
| 重试 `maxAttempts` | 3 | 含首次在内的最大尝试次数 |
| `backoffMillis` | 50ms | 线性退避基数 |
