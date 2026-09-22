# 服务发现与容错请求转发网关（本地可运行）

用**进程内的多个下游实例**模拟真实集群，在一个 Spring Boot 进程里完整实现：

- 服务实例的**注册 / 注销 / 周期健康探测**，连续失败摘除、恢复后回流；
- 在健康实例间按声明策略（**轮询 / 随机**）分发，健康集合变化时平滑收敛；
- **受限度重试**与**幂等去重**：可安全重试才重试，同一幂等标识并发请求只执行一次；
- 下游**故障隔离与熔断**：超时 / 连接拒绝 / 下游错误映射为可区分的失败类型，
  熔断打开快速失败，半开探测成功自动恢复、失败保持熔断；
- 每个关键判定都打印“输入 / 选择结果 / 判定依据”日志，单测同样打印，便于复现与解释。

## 1. 模块结构

| 包 | 关键类 | 职责 |
| --- | --- | --- |
| `model` | `ServiceInstance` `InstanceStatus` `FailureType` `DispatchRequest/Result` `GatewayException` | 领域模型、实例状态、可区分失败类型 |
| `registry` | `ServiceRegistry` `RegisteredInstance` | 并发注册中心：唯一条目、健康快照、摘除/恢复、每实例熔断器 |
| `health` | `HealthChecker` | 周期探测、阈值摘除、隔离期后恢复 |
| `downstream` | `DownstreamTransport` `InMemoryDownstreamTransport` `SimulatedInstance` `ScriptedBehavior` | 进程内模拟集群，产生可区分的超时/拒绝/错误 |
| `loadbalancer` | `LoadBalancer` `RoundRobinLoadBalancer` `RandomLoadBalancer` | 分发策略与健康集合变化收敛 |
| `circuit` | `CircuitBreaker` | CLOSED / OPEN / HALF_OPEN 三态机，时间源可注入 |
| `retry` | `RetryPolicy` | 最大尝试次数、退避、可重试失败类型白名单 |
| `idempotency` | `IdempotencyDeduplicator` | 相同幂等键的在途并发请求共享一次执行 |
| `gateway` | `ForwardingEngine` `ClusterManager` | 选实例→熔断判定→调用→分类→重试的编排；HTTP 管理协调 |
| `web` | `AdminController` `GatewayController` `GatewayExceptionHandler` | 本地验证用 REST 接口 |

## 2. 实例注册与健康判定规则

### 注册与注销

- 实例以 `(serviceName, instanceId)` 唯一标识；注册使用 `putIfAbsent`，
  **重复注册幂等**：不会新建第二条记录，也不会覆盖既有状态或重复计数。
- 注销是并发原子的：实例一旦从内部映射移除，立即对“健康实例快照”不可见，
  **绝不可能被转发选中**（转发只读取 `healthyInstances` 的不可变快照）。
- 每个实例的连续探测失败计数、摘除截止时间、熔断器都只挂在**唯一的条目**上，
  因此同一实例的健康状态不会被重复计入。

### 健康探测

- 探测器周期性（`gateway.health-probe-interval-millis`）对实例做探测。
- 对 `HEALTHY` 实例：**连续失败达到阈值** `gateway.health-failure-threshold`
  后，通过 `HEALTHY -> UNHEALTHY` 的 CAS 摘出转发集合并记录摘除截止时间；
  CAS 保证并发下只有一个线程真正摘除，已摘除的不会被重复摘除/重复计时。
- 对 `UNHEALTHY` 实例：摘除后进入**隔离期** `gateway.evict-duration-millis`，
  期间不再探测（避免无效压力）；隔离期过后继续探测，**探测成功即恢复**
  `UNHEALTHY -> HEALTHY`，清零连续失败计数并复位熔断器，重新参与流量。
- 同一实例同一时刻最多一个探测在执行（per-instance 锁 + 进行中集合），
  并发探测/注册/注销交替时，计数与实际探测一一对应、不重复不丢失。

实例状态机：

```
                注册
                 │
                 ▼
            ┌─────────┐  连续探测失败≥阈值(CAS)   ┌───────────┐
            │ HEALTHY │ ───────────────────────▶ │ UNHEALTHY │
            │ (在流量) │                            │ (已摘除)  │
            └─────────┘ ◀─────────────────────── └───────────┘
                 │     隔离期过后探测成功：清零+熔断复位
                 │ 注销
                 ▼
           DEREGISTERED（对健康快照不可见）
```

## 3. 分发与重试策略

- 选择入口始终是**当前健康实例快照**；轮询游标在集合收缩时按候选数取模，
  保证平滑收敛且不会选中已摘除实例；随机策略在候选中均匀随机。
- 一次请求内部维护“本次已尝试实例集合”，**重试只会换到别的健康实例**，
  不向同一实例重复放大压力。
- `RetryPolicy.maxAttempts`（含首次）是总上界，重试之间有固定退避
  `gateway.retry-backoff-millis` 削平压力。
- 只有 `safeRetry=true`（GET / 幂等请求）且失败类型在可重试白名单
  （`TIMEOUT`、`CONNECTION_REFUSED`、`DOWNSTREAM_ERROR`）时才重试：
  - `safeRetry=false` 的请求失败后**绝不静默重试**，直接返回
    `NON_RETRYABLE_REQUEST`（消息中保留原始失败类型）；
  - `CIRCUIT_OPEN`、`无健康实例`属于快速失败信号，不消耗重试预算；
  - 达到尝试上界时保留原始失败类型；有重试额度但没有可换健康实例时返回
    `RETRY_EXHAUSTED`。

### 幂等去重

- 请求携带非空 `idempotencyKey` 时，首个请求在 `IdempotencyDeduplicator`
  占位并真正执行；并发的同键请求拿到同一个在途 `CompletableFuture`，
  **等待并复用结果，绝不触发第二次下游执行**（响应中 `duplicated=true`）。
- 执行结束（成功或失败）后才释放占位，因此**串行**的同键请求会作为新一轮执行。

## 4. 故障分类与熔断恢复

失败类型 `FailureType`（HTTP 响应体的 `failureType` 字段与之同名，可区分）：

| failureType | 触发条件 |
| --- | --- |
| `TIMEOUT` | 模拟下游处理超过调用超时预算 |
| `CONNECTION_REFUSED` | 实例拒绝连接 / 不可达 |
| `DOWNSTREAM_ERROR` | 下游返回 5xx |
| `CIRCUIT_OPEN` | 实例熔断打开，请求被快速失败、未调用下游 |
| `NO_HEALTHY_INSTANCE` | 服务下没有任何健康实例 |
| `RETRY_EXHAUSTED` | 可重试请求在限度内仍失败且已无可换实例 |
| `NON_RETRYABLE_REQUEST` | 不可安全重试请求失败，未做重试 |

熔断器（每实例一个，`CircuitBreaker`，时间源可注入以便确定性测试）：

```
CLOSED ──连续失败达到阈值(gateway.circuit-failure-threshold)──▶ OPEN
  ▲                                                             │
  │                                          打开时长到期 + 有请求 │
  │ 半开探测成功                                （仅放行1个探测）  ▼
  └──────────────────────── HALF_OPEN ◀────────────────────────┘
              半开探测失败 ──▶ 回到 OPEN 并重新计时（保持熔断）
```

- OPEN 期间 `allowRequest=false`，转发引擎**不调用下游**直接换路/快速失败；
- OPEN 持续 `gateway.circuit-open-duration-millis` 到期后，第一个请求进入
  HALF_OPEN 并独占探测，其余请求仍被拒绝；
- 探测**成功** → CLOSED（清零计数，恢复放行）；探测**失败** → OPEN（重新计时）。

## 5. 本地验证方法

### 5.1 运行单元测试（推荐，确定性、可复现）

```bash
mvn test
```

覆盖场景（测试日志均打印 `[T] 输入 / 选择结果 / 判定依据`）：

- `ServiceRegistryTest`：并发同实例注册只成功一次、摘除 CAS 唯一、并发注销与快照；
- `HealthCheckerTest`：连续失败阈值摘除、隔离期跳过探测、到期恢复回流；
- `ConcurrentProbeTest`：并发探测同一实例计数一一对应、最大并发探测数为 1；
- `CircuitBreakerTest`：CLOSED/OPEN/HALF_OPEN 转换、半开成功恢复/失败保持；
- `CircuitIntegrationTest`：熔断打开期不放大流量、半开探测恢复/失败保持；
- `ForwardingEngineTest`：重试换实例、不可安全重试不重试、次数耗尽、
  无实例快速失败、超时类型区分；
- `IdempotencyIntegrationTest`：同键并发只执行一次、串行同键可再次执行。

### 5.2 HTTP 端到端手工验证

```bash
mvn -DskipTests package
java -jar target/service-discovery-gateway-java-1.0-SNAPSHOT.jar --server.port=18090
```

管理接口：

```bash
# 注册模拟实例（behavior: NORMAL/TIMEOUT/REFUSE/ERROR）
curl -X POST localhost:18090/admin/services/orders/instances \
  -H 'Content-Type: application/json' \
  -d '{"instanceId":"n1","behavior":"NORMAL"}'

# 切换为固定行为 / 脚本行为（按调用次数依次生效，最后一个持续）
curl -X PUT localhost:18090/admin/services/orders/instances/n1/behavior \
  -H 'Content-Type: application/json' -d '{"behavior":"REFUSE"}'
curl -X PUT localhost:18090/admin/services/orders/instances/n1/behavior \
  -H 'Content-Type: application/json' -d '{"script":["REFUSE","REFUSE","NORMAL"],"latencyMillis":200}'

# 查看状态（健康集合、实例状态、连续失败、熔断态、调用/探测计数）
curl localhost:18090/admin/services/orders

# 手动触发一轮健康探测（也可依赖后台周期任务）
curl -X POST localhost:18090/admin/health/probe

# 注销
curl -X DELETE localhost:18090/admin/services/orders/instances/n1
```

转发接口：

```bash
# 可安全重试请求
curl -X POST localhost:18090/gateway/services/orders/invoke \
  -H 'Content-Type: application/json' \
  -d '{"payload":"hi","safeRetry":true}'

# 不可安全重试（POST 等）：失败不会重试
curl -X POST localhost:18090/gateway/services/orders/invoke \
  -H 'Content-Type: application/json' \
  -d '{"payload":"create","safeRetry":false}'

# 幂等：并发同 idempotencyKey 只执行一次（建议先给实例设置 latencyMillis 制造在途窗口）
for i in $(seq 1 12); do
  curl -X POST localhost:18090/gateway/services/orders/invoke \
    -H 'Content-Type: application/json' \
    -d '{"payload":"charge","idempotencyKey":"K-1","safeRetry":true}' &
done; wait
```

成功返回含 `instanceId/statusCode/attempts/duplicated`；失败返回
`{"success":false,"failureType":"...","message":"..."}`，
`failureType` 即上表的可区分失败原因。

## 6. 配置项（`application.properties` 中以 `gateway.*` 覆盖）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `gateway.policy` | `ROUND_ROBIN` | 分发策略：`ROUND_ROBIN` / `RANDOM` |
| `gateway.timeout-millis` | 1000 | 单次下游调用超时预算 |
| `gateway.max-attempts` | 3 | 可安全重试请求最大尝试次数（含首次） |
| `gateway.retry-backoff-millis` | 50 | 重试退避 |
| `gateway.circuit-failure-threshold` | 3 | 熔断连续失败阈值 |
| `gateway.circuit-open-duration-millis` | 2000 | 熔断打开时长 |
| `gateway.health-failure-threshold` | 2 | 健康探测连续失败摘除阈值 |
| `gateway.health-probe-interval-millis` | 1000 | 健康探测周期 |
| `gateway.health-timeout-millis` | 500 | 单次探测超时 |
| `gateway.evict-duration-millis` | 5000 | 摘除隔离时长，到期才再探测恢复 |
| `gateway.health-check-enabled` | true | 是否启动后台周期探测（手工触发不受影响） |
