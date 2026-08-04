# PayFlow Lab

一个可以从单机状态机一路学到真实分布式 TCC 的 Go 支付一致性实验室。项目同时保留两种运行模式：

- `memory`：零依赖、单进程，适合先理解支付状态机、幂等、回调去重和本地 Try / Confirm / Cancel。
- `distributed`：Go + MySQL 8.4 + Redis 8.8.1 + DTM 1.19.0，账户和账本是独立 HTTP 参与者，DTM 真实协调跨服务 Confirm / Cancel。

演示台：[http://localhost:8081](http://localhost:8081)。默认账户 `demo-user`，初始余额 `1,000,000` 分（¥10,000.00）。所有金额都用 `int64` 整数分表示。

> 这是学习与面试演示项目，不是生产支付系统。它没有真实渠道、鉴权、密钥管理、多机部署、限流和完整可观测体系；README 会明确区分已实现能力与生产边界。

## 一分钟运行分布式版

要求：Docker Desktop、Docker Compose v2、PowerShell。第一次构建需要拉取镜像和 Go 依赖。

```powershell
Set-Location -LiteralPath 'D:\PayFlow Lab'
.\scripts\start-distributed.ps1
.\scripts\demo-distributed.ps1
```

浏览器打开 [http://localhost:8081](http://localhost:8081)，可以继续手动创建成功交易或注入分布式故障。

停止容器但保留 MySQL / Redis 数据：

```powershell
.\scripts\stop-distributed.ps1
```

只有确定要永久删除实验数据时才执行：

```powershell
.\scripts\stop-distributed.ps1 -RemoveVolumes
```

### 启动命令的完整调用链

1. 用户执行 `.\scripts\start-distributed.ps1`。
2. `scripts/start-distributed.ps1` 在首次运行时把 `.env.example` 复制为被 Git 忽略的 `.env`，然后执行 `docker compose --env-file .env up -d --build --wait --wait-timeout 240`。
3. `Dockerfile` 用 Go 构建 `payflow-server`、`payflow-account`、`payflow-ledger` 三个静态二进制；Compose 启动 MySQL、Redis、DTM 和三个 Go 服务。
4. MySQL 首次初始化依次执行 `infra/mysql/001-databases.sh`、`002-payflow.sql`、`003-dtm.sql`；健康检查通过后，账户和账本参与者、DTM、协调器按依赖关系启动。
5. `/app/payflow-server` 进入 `cmd/server/main.go`。`PAYFLOW_MODE=distributed` 使它创建 MySQL Repository、Redis Cache 和 `distributed.Service`，再由 `httpapi.Handler` 暴露 HTTP API。
6. 脚本最后调用 `GET http://localhost:8081/healthz`；只有 MySQL、Redis、DTM、账户、账本全部可达时才报告成功。

### 演示命令的完整调用链

1. 用户执行 `.\scripts\demo-distributed.ps1`。
2. `scripts/demo-distributed.ps1` 用 `Invoke-RestMethod` 依次检查系统、重置演示数据、创建成功交易、重放幂等请求、注入 `ledger_try` 与 `after_account_try` 两个故障点、检查余额/账本和 DTM/Barrier 证据，最后并发发送 8 个同幂等键请求。
3. 创建请求进入 `POST /api/v1/distributed/payments`，调用链为：

   ```text
   PowerShell / Browser
     → httpapi.Handler
     → distributed.Service
     → MySQL 创建 payment + 唯一幂等约束
     → dtmcli.TccGlobalTransaction2
     → DTM Server
     → account Try / Confirm / Cancel
     → ledger Try / Confirm / Cancel
     → dtmcli.BranchBarrier.CallWithDB
     → 各参与者本地 MySQL 事务
     → coordinator 核验 DTM 与分支结果后终结 payment
   ```

4. 同幂等键重放会先命中 Redis，但协调器仍按 Redis 中的 payment ID 回查 MySQL；Redis 不是正确性来源。
5. `ledger_try` 返回 DTM 业务失败，DTM 对已成功的 account Try 发起 Cancel；`after_account_try` 则验证协调器在第一个分支后中断时同样完成补偿。脚本证明可用余额恢复、冻结余额归零、没有额外 `POSTED` 账本，并打印 `dtm_status=failed` 与 Barrier 行。
6. 8 个独立 PowerShell Job 同时提交相同幂等键，脚本断言只生成一个 payment、一个 DTM GID 和一条新增 `POSTED` 账本；这条用例真实撞击 MySQL 唯一约束，不只是顺序重放。

## 分布式架构

```text
                         ┌──────── Redis 8.8.1
Browser / PowerShell ───►│ payflow :8081
                         │ Coordinator + API
                         └──────┬──────────────┐
                                │ dtmcli TCC    │ MySQL transaction
                                ▼               ▼
                         DTM :36789          MySQL 8.4
                          │      │           payflow / dtm /
                    HTTP branch  │           dtm_barrier schemas
                          │      │
          ┌───────────────┘      └───────────────┐
          ▼                                      ▼
 account :8082                            ledger :8083
 Try / Confirm / Cancel                   Try / Confirm / Cancel
 BranchBarrier + local SQL                BranchBarrier + local SQL
```

对宿主机公开的端口都绑定在 `127.0.0.1`：

| 服务 | 宿主端口 | 用途 |
|---|---:|---|
| PayFlow | `8081` | Web 与公共 API |
| MySQL | `3307` | 本地调试 |
| Redis | `6380` | 本地调试 |
| DTM HTTP | `36789` | DTM 状态与调试 |

账户 `8082` 和账本 `8083` 只在 Compose 内网开放，浏览器不能直接访问分支接口。

### 为什么 DTM URL 带 `/api/dtmsvr`

容器内配置必须是 `PAYFLOW_DTM_URL=http://dtm:36789/api/dtmsvr`。DTM Go 客户端会在这个基地址后拼接 `/newGid`、`/prepare` 等路径；只写 `http://dtm:36789` 会请求错误路径。

DTM 1.19.0 还要求 `RETRY_INTERVAL >= 10`，因此 Compose 使用 `RETRY_INTERVAL=10`；`TRANS_CRON_INTERVAL=1` 负责更频繁扫描待推进事务。

## 已守住的关键不变量

### 1. 幂等创建

- MySQL `payments.idempotency_key` 唯一索引是最终防线。
- 业务参数会生成 fingerprint；同键同参数返回原支付，同键不同参数返回 `409 idempotency_conflict`。
- Redis 只缓存 `idempotency key → payment ID + fingerprint`，缓存命中仍回查 MySQL。缓存丢失、过期或重启不会允许重复支付。
- 内存模式额外按幂等键加进程内锁，避免并发请求读到创建与冻结之间的短暂状态；它不被描述成分布式锁。

### 2. 账户 Try / Confirm / Cancel

- Try：锁定账户行，校验余额，在同一本地事务中减少 available、增加 frozen、创建唯一 reservation。
- Confirm：仅对 `FROZEN` reservation 扣减 frozen 并标记 `CONFIRMED`。
- Cancel：仅对 `FROZEN` reservation 恢复 available、扣减 frozen 并标记 `CANCELLED`。
- 每个余额更新都检查 `RowsAffected == 1`；若不满足，整个 Barrier 事务回滚并报告不变量破坏。

### 3. 账本唯一落账

- ledger Try 创建 `PREPARED` intent。
- Confirm 把 intent 推进为 `POSTED`；公共账本 API 只返回 `POSTED` 记录。
- Cancel 把 intent 推进为 `CANCELLED`，失败事务不会成为有效账本。
- 数据库唯一约束阻止同一支付重复落账。

### 4. BranchBarrier

参与者从 DTM 查询参数构造 `BranchBarrier`，校验请求体 GID 与 Barrier GID 一致，并通过 `CallWithDB` 把屏障记录和业务 SQL 放在同一 MySQL 事务中。它处理重复调用、空补偿和悬挂等常见 TCC 异常，而不是靠 Controller 内的布尔值模拟。

### 5. 终态必须有证据

协调器收到 TCC 错误后不会立刻把支付写成 `FAILED`。只有同时证明：

- DTM 全局状态为 `failed`；
- 账户 reservation 不再是 `FROZEN`；
- 账本 intent 不是 `POSTED`；

才终结为 `FAILED`。如果等待超时，支付继续保持 `PROCESSING`，记录 `DTM_RECONCILE_PENDING` 事件；后续详情查询/协调逻辑再次核验并收敛。成功终态同样要求 DTM `succeed` 且两个分支结果成立。

### 6. 安全重置范围

`POST /api/v1/demo/reset` 首先拒绝存在 `PROCESSING` 支付的重置。它会清理 PayFlow 自己的支付、事件、reservation 和 ledger 表；对共享的 DTM / Barrier 表，只按 PayFlow payments 中出现过的 GID 定向删除，不执行全表清空，也不删除 DTM 的其他业务记录。Redis 只清理 `payflow:` 前缀键。

## 分布式 API 与故障实验

所有 API 都使用统一 envelope：

```json
{"success":true,"data":{}}
```

```json
{"success":false,"error":{"code":"validation_error","message":"..."}}
```

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/healthz` | 服务与五项依赖健康检查 |
| `GET` | `/api/v1/system` | 当前模式和支持的故障点 |
| `POST` | `/api/v1/payments` | 创建支付；内存模式支持回调状态机，分布式模式仍走真实 TCC |
| `POST` | `/api/v1/distributed/payments` | 发起真实 DTM TCC 支付 |
| `GET` | `/api/v1/distributed/transactions/{gid}` | 查看 DTM、reservation、ledger intent、Barrier 证据 |
| `GET` | `/api/v1/payments` | 支付列表 |
| `GET` | `/api/v1/payments/{id}` | 支付详情与事件 |
| `GET` | `/api/v1/payments/{id}/events` | 单独查看有序状态机事件 |
| `POST` | `/api/v1/payments/{id}/callbacks` | 内存模式演示回调幂等与乱序保护；分布式模式拒绝此入口 |
| `POST` | `/api/v1/payments/{id}/close` | 内存模式演示安全关单；分布式模式拒绝此入口 |
| `GET` | `/api/v1/accounts/{id}` | 账户余额 |
| `GET` | `/api/v1/ledger` | 仅查看有效 `POSTED` 账本 |
| `GET` | `/api/v1/metrics` | JSON 指标 |
| `POST` | `/api/v1/demo/reset` | 安全重置本项目实验数据 |

创建成功交易：

```powershell
$body = @{
  order_id = 'order-1001'
  account_id = 'demo-user'
  amount_cents = 1999
  currency = 'CNY'
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
  -Uri 'http://localhost:8081/api/v1/distributed/payments' `
  -Headers @{ 'Idempotency-Key' = 'idem-order-1001' } `
  -ContentType 'application/json' -Body $body | ConvertTo-Json -Depth 8
```

可注入两个分布式故障：

- `after_account_try`：账户已 Try 后让全局事务失败，用于观察已冻结资金被 Cancel 恢复。
- `ledger_try`：账本 Try 返回业务失败，DTM Cancel 已完成的账户分支。

```powershell
$faultBody = @{
  order_id = 'order-fault-1001'
  account_id = 'demo-user'
  amount_cents = 2500
  currency = 'CNY'
  fault = 'ledger_try'
} | ConvertTo-Json

$result = Invoke-RestMethod -Method Post `
  -Uri 'http://localhost:8081/api/v1/distributed/payments' `
  -Headers @{ 'Idempotency-Key' = 'idem-fault-1001' } `
  -ContentType 'application/json' -Body $faultBody

$gid = $result.data.payment.gid
Invoke-RestMethod -Method Get `
  -Uri "http://localhost:8081/api/v1/distributed/transactions/$gid" |
  ConvertTo-Json -Depth 12
```

## 测试

本地单元/HTTP 回归测试：

```powershell
Set-Location -LiteralPath 'D:\PayFlow Lab'
.\scripts\test.ps1
```

完整分布式集成测试：

```powershell
.\scripts\test-distributed.ps1 -StopAfter
```

如果容器已经构建过，可以缩短启动：

```powershell
.\scripts\test-distributed.ps1 -NoBuild -StopAfter
```

### 测试命令的完整调用链

1. 用户执行 `.\scripts\test-distributed.ps1 -StopAfter`。
2. `scripts/test-distributed.ps1` 执行 `go test -count=1 ./...`，Go 自动发现各包的 `*_test.go` 并运行内存状态机与真实 HTTP 契约回归。
3. 脚本执行 `docker compose --env-file .env config --quiet` 检查 Compose 展开结果。
4. 脚本调用 `scripts/start-distributed.ps1`；后者执行完整 build → up → wait → health 链路。
5. 脚本调用 `scripts/demo-distributed.ps1`，通过真正的 HTTP、DTM 和 MySQL 执行 Confirm 与 Cancel 断言。
6. `-StopAfter` 使 `finally` 调用 `scripts/stop-distributed.ps1`，最终执行 `docker compose --env-file .env down --remove-orphans`；默认保留 named volumes。

`stop-distributed.ps1 -RemoveVolumes` 会额外向最后一条 Docker 命令加入 `--volumes`，永久删除本项目 MySQL / Redis named volumes，脚本会先输出警告。

## 运行轻量内存版

只要求 Go 1.24+ 和 PowerShell，不需要 Docker：

```powershell
Set-Location -LiteralPath 'D:\PayFlow Lab'
.\scripts\start.ps1
```

另开终端：

```powershell
.\scripts\demo.ps1
```

完整启动链路为：用户执行 `start.ps1` → 脚本设置 `PAYFLOW_ADDR` → 执行 `go run ./cmd/server` → `cmd/server/main.go` 默认选择 `PAYFLOW_MODE=memory` → 创建 `MemoryStore` 和 `PaymentService` → `http.Server.ListenAndServe()`。完整演示链路为：用户执行 `demo.ps1` → `Invoke-RestMethod` → `httpapi.Handler` → `PaymentService` → `MemoryStore`。

内存模式支持：

- 状态机 `INIT → PROCESSING → SUCCESS / FAILED / CLOSED`，终态不可回退；
- 幂等创建、同键不同参数冲突；
- 回调 `event_id` 去重、`sequence` 乱序保护、终态保护；
- `before_freeze`、`after_freeze`、`before_confirm` 故障注入；
- 本地 Try / Confirm / Cancel 和事件时间线。

进程退出后内存数据会丢失，本地 TCC 也不具备跨服务原子性。这正是它和 distributed 模式的教学对照。

## 配置与目录

`.env.example` 里的密码只用于本机学习，请勿复制到公网或生产环境。可调整的宿主端口：

```dotenv
PAYFLOW_MYSQL_PORT=3307
PAYFLOW_REDIS_PORT=6380
PAYFLOW_DTM_PORT=36789
PAYFLOW_HTTP_PORT=8081
```

主要目录：

```text
PayFlow Lab/
├─ cmd/server/                 # 协调器与公共 API 入口
├─ cmd/account/                # 账户 TCC 参与者入口
├─ cmd/ledger/                 # 账本 TCC 参与者入口
├─ internal/distributed/       # MySQL、Redis、DTM 编排、诊断与收敛
├─ internal/participant/       # BranchBarrier 与账户/账本本地事务
├─ internal/service/           # 内存模式支付状态机
├─ internal/store/             # Store 接口与内存实现
├─ internal/httpapi/           # API 与嵌入式 Web 演示台
├─ infra/mysql/                # 三个 schema 的首次初始化脚本
├─ scripts/                    # 启动、演示、测试、停止入口
├─ Dockerfile
└─ compose.yaml
```

## 面试讲解建议

建议按“问题—约束—方案—证据—边界”讲，不要声称自己实现了 DTM 内核：

1. 问题：跨账户与账本服务时，单库事务不够；重复请求和超时重试还会放大风险。
2. 约束：金额必须是整数；幂等最终要由数据库唯一约束保证；失败终态必须在补偿完成后才能写入。
3. 方案：协调器用 DTM TCC，参与者用 BranchBarrier + 本地事务；Redis 只做加速；MySQL 保存支付、意图和审计事件。
4. 证据：运行 `demo-distributed.ps1`，展示 SUCCESS 的 DTM GID，再展示 `ledger_try` 下 DTM `failed`、reservation `CANCELLED`、余额恢复、唯一 POSTED ledger 和 Barrier rows。
5. 边界：单实例协调器、开发密码、没有真实支付渠道与鉴权，也没有把“容器自动重启”包装成生产级高可用。

可以准确写成简历表述：

> 使用 Go、MySQL、Redis 与 DTM 实现可运行的支付 TCC 实验系统，拆分账户/账本参与者，以 BranchBarrier 和本地事务处理重复调用、空补偿与悬挂；通过 MySQL 唯一约束实现最终幂等，Redis 仅作非权威缓存；设计故障注入及诊断接口，验证 Confirm/Cancel、余额恢复和账本唯一性。

## 版本与参考

- [DTM 官方 GitHub 仓库](https://github.com/dtm-labs/dtm)，本项目固定服务镜像 `yedf/dtm:1.19.0`，Go 客户端来自同版本模块。
- [DTM TCC 官方文档](https://en.dtm.pub/practice/tcc.html)。
- [DTM 事务屏障官方文档](https://en.dtm.pub/practice/barrier.html)。
- MySQL 镜像 `mysql:8.4`，Redis 镜像 `redis:8.8.1`。

下一阶段适合增加恢复扫描器/定时对账、Outbox、Prometheus + OpenTelemetry、回调 HMAC 与 nonce、防刷限流、真实压测和多实例故障实验。
