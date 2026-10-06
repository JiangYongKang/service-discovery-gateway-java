# Service Discovery Gateway（本地可运行的服务发现与容错转发）

用**进程内的多个下游实例**模拟真实集群，实现注册/注销、健康探测与摘除恢复、多实例负载
分发、重试与幂等去重、实例级熔断与半开恢复，并在其上叠加**灰度权重 + 容量治理**与
**按服务的重试预算**。所有关键判定都打印日志（输入、选择结果、判定依据），使实例状态
变化与故障链路**可解释、可复现**。

技术栈：Java 21、Spring Boot 3.4、JUnit 5，无外部中间件依赖。

---

## 1. 模块结构

```
discovery/
├── model/          枚举与值对象：InstanceStatus / FailureType / CircuitState /
│                   ForwardRequest / ForwardOutcome / HealthConfig / RetryPolicy /
│                   RetryBudgetPolicy ...
├── downstream/     DownstreamHandler（下游调用抽象）+ SimulatedDownstream（进程内模拟器）
├── instance/       ServiceInstance（实例状态机 + 权重/并发上限/在途名额）、
│                   ServiceRegistry / InMemoryServiceRegistry（注册/注销/探测/健康集合）、
│                   HealthProbeScheduler（周期探测）
├── loadbalancer/   LoadBalancer + RoundRobinLoadBalancer / RandomLoadBalancer /
│                   WeightedRoundRobinLoadBalancer（平滑加权 + 容量跳过）
├── circuit/        CircuitBreaker + DefaultCircuitBreaker（CLOSED/OPEN/HALF_OPEN，
│                   含失败类型分布统计）
├── retry/          IdempotencyDeduplicator + InMemoryIdempotencyDeduplicator /
│                   RetryBudget + SlidingWindowRetryBudget（滑动窗口重试预算）
├── gateway/        GatewayForwarder（发现→容量→熔断→加权分发→执行→预算→重试→去重 的总装，
│                   含只读运行状态视图）
├── config/         DiscoveryConfig（Spring Bean 装配与默认参数）
└── web/            DemoController（本地手工验证 HTTP 接口）
```

---

## 2. 实例注册与健康判定规则

### 2.1 注册 / 注销

- `register(service, instanceId)` 幂等：底层用 `ConcurrentHashMap.putIfAbsent`，
  **并发注册同一实例只有一个胜出**，健康集合不会重复计入同一实例。
- `register(service, instanceId, weight, maxConcurrency)` 可携带灰度权重与并发承载
  上限（见 3.1）；缺省 `weight=1`、`maxConcurrency=0`（不限），与老行为完全一致。
  该方法对已存在实例是**幂等忽略**（只用于“首次注册”）；要对已存在实例在线调权/
  调容量，请使用下文的 `registerOrUpdate`（见 3.1）。
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

### 3.1 负载分发（权重 + 容量）

- 支持声明式策略：`WEIGHTED_ROUND_ROBIN`（默认，平滑加权轮询）、`ROUND_ROBIN`、
  `RANDOM`。
- **权重分摊**：注册时可声明 `weight`（>=1，默认 1）。平滑加权轮询（与 nginx 同算法）
  让长期选中比例精确等于权重比，且分布平滑不扎堆；全部实例权重为 1 时退化为普通
  轮询，与既有默认行为一致。
- **在线调整已存在实例的权重 / 并发上限（灰度放量/缩量）**：对一个已经在跑的实例
  再次调用 `registerOrUpdate(service, instanceId, weight, maxConcurrency)`
  （HTTP：`POST /demo/instances/{id}/register?weight=..&maxConcurrency=..`）即可，
  **不需要也不应该先注销再注册**：
  - 实例不存在 → 新注册（返回 `outcome=REGISTERED`）；实例已存在 → 仅在线更新这两个
    运行参数（返回 `outcome=UPDATED` 及归一化后**真正生效**的值）；
  - 调整**不影响**健康状态、连续失败计数与在途占用；`weight`/`maxConcurrency` 为
    volatile 字段，更新在实例同步方法内完成，负载均衡与容量预筛的下一轮选择
    **立即按新值分摊**——调大权重后流量比例立刻随之变化，调小并发上限后多余流量
    立刻让给其它有余量实例；平滑加权轮询直接读取最新权重并自然收敛，无需重置状态。
- **容量治理**：注册时可声明 `maxConcurrency`（在途请求上限，0=不限，默认不限）。
  - 每轮选择前做容量预筛：在途数已达上限的实例本轮跳过（日志 `CAPACITY_FULL`），
    它的权重份额按比例让给其余**还有余量**的健康实例；
  - 选中后以 CAS `tryAcquireInFlight` **原子占用名额**，并发下绝不超发；
    预筛后名额被并发抢走时本轮排除该实例并重选；
  - 请求结束（**无论成功、超时还是下游报错**）都在 `finally` 中归还名额，
    不会因计数泄漏把实例永久判成满载；名额空出后实例自动回到分发。
- 选择只依赖“**当前健康快照 ∩ 有余量 ∩ 熔断放行 ∩ 本次未尝试**”的候选集合：
  - 健康集合缩小时自然收敛，不会选到已摘除实例；
  - 重试时通过 `excluded` 排除本次已尝试实例，**不把重试压力重新打到故障实例**。
- 日志可复盘每一轮：各候选的权重与平滑计数值、谁被选中、谁因满载被跳过，例如
  `lb input: ... -> choose=i1 (basis: smooth WRR [i1(w=5,cur=5) i2(w=1,cur=1)], ...)`。

### 3.2 失败类型映射（彼此可区分）

| 现象               | FailureType          | 是否真实到达下游 | 默认是否自动重试 |
|--------------------|----------------------|------------------|------------------|
| 调用超时           | `TIMEOUT`            | 否/不确定        | 是（传输故障）   |
| 连接拒绝           | `CONNECTION_REFUSED` | 否               | 是（传输故障）   |
| 下游返回 4xx/5xx   | `DOWNSTREAM_ERROR`   | 是（业务已执行） | **否**，防重复执行 |
| 无健康实例         | `NO_HEALTHY_INSTANCE`| 否               | 否（快速失败）   |
| 候选全部熔断打开   | `CIRCUIT_OPEN`       | 否               | 否（快速失败）   |
| 候选全部满载       | `CAPACITY_FULL`      | 否               | 否（快速失败）   |
| 可重试请求次数用尽 | `RETRY_EXHAUSTED`    | —                | 终态归并         |

### 3.3 重试规则（防止放大下游压力）

- 只有请求显式声明 `retryable=true` 才允许重试；**不可安全重试的请求只执行一次，
  绝不静默重试**，结果中原样保留真实失败类型与“exactly 1 attempt”的判定依据。
- 最多 `maxAttempts` 次尝试（默认 **3**：1 次正常 + 2 次重试），次数耗尽归并为
  `RETRY_EXHAUSTED`，但每次尝试的实例与结果明细都保留在 `attempts` 中。
- 仅对 `TIMEOUT` / `CONNECTION_REFUSED` 两类传输故障重试；5xx 不自动重试。
- 线性退避 `backoffMillis * attempt`（默认 50ms 起），且仅当“**还有别的健康且
  熔断放行的实例**”时才重试，否则提前终止，不做无意义重试。

### 3.4 按服务的重试预算（滑动窗口，默认关闭）

在既有单请求重试上限之上，可为每个服务加一层**故障期总量闸门**
（`RetryBudgetPolicy(windowMillis, maxRetriesInWindow, maxRetryRatio)`）：

- 滑动窗口内，该服务的**自动重试**总次数不得超过 `maxRetriesInWindow`，
  且 重试数/首发数 不得超过 `maxRetryRatio`（`>=1.0` 视为不限制占比）；
- **预算只拦截自动重试**：耗尽后新来的请求**照常发首发**，只是不再自动换实例
  重试——避免故障期重试风暴把整片下游打垮；首发请求永远不会被预算判成失败；
- 窗口滑过后预算自动恢复，无需人工干预；
- 判定与记账在同一同步块内完成，**并发下不超发**；每次判定都打印依据
  （`retry budget decision: ... ALLOW/DENY (...)`），被拦截时结果保留真实失败
  类型，`reason` 中注明 `auto-retry suppressed by retry budget: ...`；
- **默认 `RetryBudgetPolicy.disabled()`：不启用预算，行为与既有版本完全一致，
  不改变任何既有用法**；在 `DiscoveryConfig#retryBudgetPolicy` 中改为例如
  `new RetryBudgetPolicy(10_000, 20, 0.2)` 即可启用（10 秒窗内重试不超过 20 次
  且不超过首发量的 20%）。

### 3.5 幂等去重

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
- **谁来触发恢复探测（多实例同时到期也互不影响）**：冷却到点后有两条触发路径，
  二者都通过熔断器的 `tryAcquireRecoveryProbe` **原子领取**同一把“单实例探测锁”：
  1. **后台周期恢复扫描**（默认随健康探测每 500ms 一轮，由
     `GatewayForwarder.sweepRecoveryProbes` 执行）：对该服务中**每一个**冷却到点
     （或半开探测失活）的实例，各自独立提交一个真实探测任务并行执行。
     **多个实例的冷却同时到期时，每个实例都会真正收到一次下游探测**——它们各自
     领取、并行探测、互不阻塞，不会只恢复其中一个、其余永远停在“正在探测”；
  2. **真实转发请求**：门控时发现某实例冷却到期，会在本轮原子领取并优先把这次请求
     作为唯一探测发出（领取与权重无关，**低权重实例冷却到点也一定会被探测**）。
- **同一实例探测窗口内恰好只有一个探测**：领取动作是原子的，后台扫描与并发涌入的
  业务请求同时发生时，同一实例也只有一个能领取成功，其它请求照常快速失败/走健康
  实例，不会一拥而上重复探测；不同实例之间互不影响。
- **失活占坑回收**：若半开探测超过 `probeStaleMillis`（默认 2000ms）仍无结果返回
  （占坑线程被杀、调用方异常退出等），判定该探测丢失，下一轮扫描可重新领取，
  实例不会永久卡在 `HALF_OPEN` 而再也等不到探测。
- 半开探测成功次数达到 `trialSuccessRequired` → 立即回 `CLOSED`，**立刻重新接流量**；
- 半开探测**任一失败 → 立即回 `OPEN` 并以当前时刻重新计时**，必须再等一个完整冷却
  周期才会有下一次探测，新冷却期内扫描与流量都不会再探测它；
- 熔断统计只计入“真实执行过”的尝试；被熔断拦截本身不计为下游失败。
- 健康摘除（注册中心视角）与熔断（转发器视角）是两道相互独立的防线，可叠加生效。

### 4.1 只读运行状态视图

`GatewayForwarder.statusView(service)`（HTTP：`GET /demo/status`）按实例返回：

| 字段 | 含义 |
|------|------|
| `circuitState` | 当前熔断状态（CLOSED / OPEN / HALF_OPEN） |
| `probing` | 是否正处于半开探测窗口（探测请求在途） |
| `failureTypeDistribution` | **最近 `failureWindowMillis`（默认 10s）时间窗内**的失败类型分布；是“最近口径”，旧失败滑出窗口后自动不再出现，**不是**从启动起一路累加的历史累计值 |
| `circuitOpenSkips` | 因熔断打开被跳过的累计次数 |
| `capacityFullSkips` | 因满载被跳过的累计次数 |
| `weight` / `maxConcurrency` / `inFlight` | 权重、并发上限与当前在途数 |

视图只读取快照与计数器（失败分布只按时间窗过滤，不增删记录），**不调用任何会改变
状态的方法**（如 `allowRequest` / `tryAcquireRecoveryProbe`），查看本身不影响熔断
恢复、探测节奏与流量分发，可反复调用。失败窗口由
`CircuitBreakerConfig.failureWindowMillis` 配置。

---

## 5. 本地验证方法

### 5.1 运行单元测试（42 个，覆盖需求全部关键场景）

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
| `WeightedCapacityTest` | 权重 3:1 精确分摊；并发下在途名额不超发、成功/超时/500 均归还名额不泄漏；满载让份额、空下来自动回归分发 |
| `RetryBudgetTest` | 窗口内预算耗尽只拦重试不拦首发、占比上限、窗口滑过后自动恢复、32 并发抢预算不超发 |
| `CircuitRecoveryChainTest` | 熔断→冷却→经权重/容量筛选后真实探测→恢复接流量；探测失败重新计时；探测窗口内并发不一拥而上；只读视图不触发翻牌 |
| `RecoveryProbeSweepTest` | 多实例冷却同时到期各自恰好一次真实探测并独立恢复；探测失败重新计时、新冷却期内不再探测；低权重实例冷却到点仍被真实探测；在线调整已存在实例权重后分流按新值变化；失败类型分布为最近窗口口径（旧计数滑出消失）；扫描与并发流量同发时单实例窗口内仍只有一个探测 |

测试与主流程均通过 SLF4J 打印**输入、选择结果与判定依据**，例如：

```
lb input: service=svc, policy=WEIGHTED_ROUND_ROBIN, available=2, excluded=[] -> choose=i1
  (basis: smooth WRR [i1(w=5,cur=5) i2(w=1,cur=1)], winner cur=5 -= totalWeight=6)
lb skip: service=svc, instance=i1 reason=CAPACITY_FULL (inFlight=1/max=1), its weight share goes to remaining instances
probe result: instance=i1 -> CONNECTION_REFUSED; decision=EVICT(reached 3 consecutive failures, ...)
circuit[i1] OPEN -> HALF_OPEN: elapsed=2000ms >= openMillis=2000ms, exactly ONE trial request allowed
probe priority: instance=i1 just flipped OPEN->HALF_OPEN, selected as the single trial request
retry budget decision: service=svc -> DENY (retry budget exhausted: retries=2/2 within window=10000ms; ...)
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

# 1) 注册 3 个进程内模拟实例（默认 weight=1、不限并发，与老行为一致）
curl -X POST "$B/register?count=3"

# 1b) 灰度放量：注册一个权重 5、并发上限 2 的实例
curl -X POST "$B/instances/inst-4/register?weight=5&maxConcurrency=2"

# 1c) 对【已在跑】的实例在线调大权重/并发上限（outcome=UPDATED，下一轮分流立即按新值）
curl -X POST "$B/instances/inst-4/register?weight=20&maxConcurrency=8"
curl "$B/status"        # weight/maxConcurrency 已变为新值

# 2) 正常转发（加权轮询；全部权重为 1 时等同普通轮询）
curl -X POST "$B/forward" -H 'Content-Type: application/json' \
  -d '{"payload":"hello","retryable":true}'

# 3) 制造故障：NORMAL / REFUSE / TIMEOUT / ERROR_500
curl -X POST "$B/instances/inst-2/mode?mode=REFUSE"
curl -X POST "$B/instances/inst-3/mode?mode=TIMEOUT"

# 4) 立即触发一轮健康探测（连续 3 轮后 inst-2/inst-3 被摘除；冷却 3 秒）
curl -X POST "$B/probe"

# 4b) 立即触发一轮熔断恢复探测：所有冷却到点/探测失活的实例各自真实探测一次
#     （多实例并行、互不阻塞；同实例探测窗口内只有一个；返回 dueInstancesProbed）
curl -X POST "$B/recovery-probe"
curl "$B/status"        # 成功的实例 circuitState 回到 CLOSED、probing=false

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

# 9) 只读运行状态视图：熔断/探测中/失败类型分布/熔断与满载跳过累计/在途名额
#    （查看本身不改变任何状态，可反复调用）
curl "$B/status"
```

`ForwardOutcome` 的每一次返回都包含 `success / response / failureType / reason /
attempts`（每次尝试的实例、序号、结果），可直接看到选择序列与失败判定依据；
应用日志中同步输出每一步的判定过程。

#### 多实例同时恢复 / 在线调权 / 最近失败分布 的手工验证

```bash
B=http://localhost:8080/demo
curl -X POST "$B/register?count=3"                                   # i1/i2/i3

# 1) 三个实例同时故障，发若干请求把它们一起熔断
for i in 1 2 3; do curl -X POST "$B/instances/inst-$i/mode?mode=REFUSE"; done
for i in $(seq 1 20); do curl -s -X POST "$B/forward" \
  -H 'Content-Type: application/json' -d '{"payload":"p","retryable":false}' >/dev/null; done
curl "$B/status"   # 三个实例 circuitState 都应为 OPEN

# 2) 下游恢复：等冷却到期（或直接触发恢复扫描），三个实例各自被真实探测一次并恢复
for i in 1 2 3; do curl -X POST "$B/instances/inst-$i/mode?mode=NORMAL"; done
sleep 2.5   # 等待周期恢复扫描；或立即 curl -X POST "$B/recovery-probe"
curl "$B/status"   # 三个实例都应回到 CLOSED（不会只有一个恢复、其余一直 HALF_OPEN）

# 3) 在线调大已存在实例权重后，分流比例立即变化
curl -X POST "$B/instances/inst-1/register?weight=10&maxConcurrency=0"  # outcome=UPDATED
for i in $(seq 1 24); do curl -s -X POST "$B/forward" \
  -H 'Content-Type: application/json' -d '{"payload":"q","retryable":false}' \
  | grep -o 'inst-[0-9]'; done | sort | uniq -c                         # inst-1 约占 10/12

# 4) 最近失败分布：制造失败后查看 failureTypeDistribution；静置 10 秒后再看，旧计数滑出
curl -X POST "$B/instances/inst-2/mode?mode=TIMEOUT"
for i in $(seq 1 6); do curl -s -X POST "$B/forward" \
  -H 'Content-Type: application/json' -d '{"payload":"r","retryable":false}' >/dev/null; done
curl "$B/status" | python3 -m json.tool   # inst-2 的 failureTypeDistribution.TIMEOUT
sleep 11
curl "$B/status" | python3 -m json.tool   # 窗口滑过后该计数归零（查看本身不改变状态）
```

恢复扫描与领取相关日志示例：

```
recovery sweep: service=demo-service, dueInstancesProbed=3/3, each instance probed independently in parallel
circuit[inst-1] OPEN -> HALF_OPEN: cooldown elapsed=2001ms >= openMillis=2000ms, recovery probe claimed (exactly ONE in flight)
circuit[inst-2] OPEN -> HALF_OPEN: cooldown elapsed=2003ms >= openMillis=2000ms, recovery probe claimed (exactly ONE in flight)
recovery probe result: instance=inst-1 -> UP status=200; decision=CLOSED, traffic restored immediately
circuit[inst-1] HALF_OPEN -> CLOSED: trial success (1/1), traffic restored
recovery probe result: instance=inst-3 -> CONNECTION_REFUSED; decision=OPEN, cooldown restarted (no probe again until new cooldown elapses)
register-or-update input: ... weight=20 ... -> result=UPDATED(existing,status=HEALTHY), effectiveWeight=20, next selection round applies new values immediately
```

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
| `probeStaleMillis` | 2000ms | 半开探测多久无结果视为失活，后台扫描重新领取（防永久卡在 HALF_OPEN） |
| `failureWindowMillis` | 10000ms | 只读视图失败类型分布的“最近窗口”长度 |
| 重试 `maxAttempts` | 3 | 含首次在内的最大尝试次数 |
| `backoffMillis` | 50ms | 线性退避基数 |
| 注册 `weight` | 1 | 灰度权重（平滑加权轮询的分摊比例） |
| 注册 `maxConcurrency` | 0（不限） | 单实例在途请求上限，0 表示不启用容量治理 |
| 重试预算 `RetryBudgetPolicy` | `disabled()` | 默认不启用；启用后按滑动窗口限制自动重试总量/占比 |

> **兼容性说明**：以上三项新能力（权重、容量、重试预算）的默认值都与既有行为完全
> 一致——权重 1 + 不限并发时加权轮询退化为普通轮询；`RetryBudgetPolicy.disabled()`
> 时重试判定链与旧版逐条相同。既有 HTTP 接口与 Java 构造器签名均保持兼容。
