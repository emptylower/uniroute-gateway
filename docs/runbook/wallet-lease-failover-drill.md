# 钱包租约故障演练与灾备恢复操作手册 (Wallet Lease Failover Drill & Disaster Recovery Runbook)

> 类型：Runbook
> 归属：钱包租约 Phase 4.3 — 生产就绪

**版本：** 2.0 (Phase 4.3-G)  
**目标组件：** Sub2API Unirouter — Canonical Wallet Subsystem  
**演练范围：** Redis 宕机中断、Redis 主从副本切换（Replica Promotion Mid-Hold）、网关异常崩溃、PostgreSQL 灾难恢复及跨控制面 penny-exact 对账。

---

## 0. 演练前置条件 (Preconditions)

1. **Docker Compose 环境**：`docker compose --env-file deploy/.env -f deploy/docker-compose.yml up -d redis postgres` 已就绪。
2. **模式与策略**：`CANONICAL_WALLET_MODE=enforce`，`canonical_wallet_redis_policy` 校验通过（`maxmemory-policy=noeviction`）。
3. **资金与用户**：测试用户在 ShipAny 控制面与 Sub2API 数据面已完成充值与授权。
4. **对账凭据**：Sub2API 与 ShipAny 均配置有长度 ≥ 32 字节的 `GATEWAY_WALLET_SUB2API_RECONCILIATION_TOKEN`。

---

## 1. 架构与容灾模型 (Subsystem Architecture Overview)

Canonical Wallet 子系统在数据面（Sub2API）与控制面（ShipAny）之间提供强一致、无单点故障的信用租赁管理：
- **Redis (数据面易失状态)**：
  - 活跃租约哈希：`canonical_wallet:lease:{user}:{lease_id}`
  - Hold 状态哈希：`canonical_wallet:hold:{user}:{auth_id}`
  - 用户 Hold 索引：`canonical_wallet:holds:{user}`、全局用户集 `canonical_wallet:holds_users`
  - 孤儿扫描全局分布式锁：`canonical_wallet:reaper:tick`
- **PostgreSQL (数据面持久化状态)**：
  - `wallet_settlement_outbox`：至少一次投递保证的控制面回写 Outbox。
  - `wallet_hold_outcome`：所有 Hold 的最终归宿（`settled`、`abandoned`、`expired`、`released`）。
  - `wallet_live_provisional`：流式会话 provisional window 状态追踪。
  - `wallet_billing_snapshot`：不可变计费快照。
- **孤儿清理协调器 (Orphan Sweeper / Reaper)**：
  - 周期性 Tick 驱动，依据 `orphan_grace_seconds` 判定并清理超时未结算的 Hold，记录 Outcome 并释放用户额度。

---

## 2. 故障场景演练流程 (Drill Scenarios)

### Scenario A: Redis 宕机与自动重连恢复 (Redis Outage)

#### 1. 故障注入
- 在会话活跃且 Hold 已 Arm 的状态下，强制停止 Redis 服务：
  ```bash
  docker compose -f deploy/docker-compose.yml stop redis
  ```

#### 2. 行为验证与断言
- **Fail-Closed 保护**：`mode=enforce` 下所有新的授权请求均立即拒绝，返回 `AuthorizationRefusalLeaseUnavailable`（HTTP 402/429 对应语义），拒绝穿透至上游。
- **Shadow 宽容**：`mode=shadow` 下请求正常放行，记录 `canonical_wallet_live_window_refused_shadow_total` 观测指标。
- **结算事件持久化**：宕机期间上报的结算事件安全写入 PostgreSQL `wallet_settlement_outbox`，状态保持为 `pending` 或 `in_flight`，**零 Dead-letter，零数据丢失**。

#### 3. 故障恢复
- 重新启动 Redis 容器：
  ```bash
  docker compose -f deploy/docker-compose.yml start redis
  ```
- **自动恢复**：go-redis 客户端连接池自动重连同一主机端口，无需重启网关进程。
- **Outbox 排空**：Outbox 调度器在下一个 Tick 周期内自动投递断网期间堆积的结算，全部转换为 `delivered` 状态。

---

### Scenario B: Hold 期间 Redis 主从切换 (Replica Promotion Mid-Hold)

#### 1. 故障注入
- 部署 Redis 主从对（Primary 端口 16384，Replica 端口 16385），Replica 执行 `REPLICAOF` 挂载至 Primary。
- 在 Primary 节点上为用户并发创建 3 笔活跃 Hold（`auth-1`、`auth-2`、`auth-3`）。
- 确认 Hold 数据同步至 Replica 节点。
- 模拟 Primary 宕机，向 Replica 发送提升指令：
  ```bash
  redis-cli -p 16385 REPLICAOF NO ONE
  ```
- 实例化新 Bridge（模拟新拉起的网关进程连接 Promoted Master 节点）。

#### 2. 行为验证与断言
- **结算一致性**：
  - `auth-1` 通过新 Bridge 正常完成结算转换，Outbox 投递成功，记录 `wallet_hold_outcome` 为 `settled/converted`。
  - `auth-2` 通过零成本释放逻辑成功解冻额度，记录 Outcome 为 `released`。
  - `auth-3` 超过 `orphan_grace_seconds` 后由 Promoted Master 上的 Orphan Reaper 扫描回收，记录 Outcome 为 `abandoned`。
- **精确一次解析 (Exactly-Once Resolution)**：
  - 跨新旧两个 Bridge，每个 Hold **仅解析一次**，`wallet_hold_outcome` 每个授权 ID 恰好生成 1 条记录，无重复扣费，无孤儿悬挂。
  - §13.2.5 fallback-bucket 损耗有界，Prometheus 计数器 `queue_dropped` 严格为 0。

---

## 3. 可观测性与核查命令 (Verification & Observability Commands)

### A. Prometheus 监控指标
```bash
# Reaper 运行状况与错误计数
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_reaper_ticks_total
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_reaper_error_total

# Hold 结算、释放与放弃计数
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_holds_abandoned_total
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_holds_converted_total
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_hold_outcomes_expired_total

# Outbox 投递与丢弃指标
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_settlements_delivered_total
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_queue_dropped_total
```

### B. Redis 状态核查
```bash
# 确认内存淘汰策略为 noeviction
redis-cli -p 6379 config get maxmemory-policy

# 查询用户当前活跃 Hold
redis-cli -p 6379 smembers canonical_wallet:holds:<platform_user_id>
```

### C. PostgreSQL 数据一致性核查
```sql
-- 1. 确认无死信结算
SELECT count(*) AS dead_letters FROM wallet_settlement_outbox WHERE status = 'dead_letter';

-- 2. 确认 Outbox 全部完成投递
SELECT status, count(*) FROM wallet_settlement_outbox GROUP BY status;

-- 3. 查询 Hold Outcome 最终状态分布
SELECT resolution, count(*) FROM wallet_hold_outcome GROUP BY resolution;
```

### D. ShipAny 跨控制面对账
```bash
cd /Users/mac/project/all-model-router
GATEWAY_WALLET_SUB2API_URL=http://localhost:8080 \
GATEWAY_WALLET_SUB2API_RECONCILIATION_TOKEN=<configured_token_32b> \
pnpm wallet:reconcile
```

---

## 4. 结论断言表 (Verdict Table & Invariant Guarantees)

| 场景 / 断言项 | 预期行为 | 生产保证 | 演练结果 |
|---|---|---|---|
| **Redis 宕机中新请求** | Enforce 模式拒绝 `lease_unavailable`；Shadow 模式放行 | 杜绝上游击穿与资金透支 | PASS |
| **断网期间结算 Outbox** | 写入 PostgreSQL，状态保持 `pending`，0 dead-letter | 至少一次投递，绝不丢账 | PASS |
| **Redis 恢复后 Outbox** | 自动恢复投递，全部转为 `delivered` | 自愈收敛，无需人工干预 | PASS |
| **副本提升 (Promotion)** | 新 Bridge 接管 Promoted Master，Hold 状态保持 | 业务平滑切换，无缝接管 | PASS |
| **跨主从 Hold 解析** | 每个 Hold 恰好解析一次（3/3 分别 converted/released/abandoned） | 零双重扣费，无孤儿悬挂 | PASS |
| **Fallback-Bucket 损耗** | `queue_dropped = 0`，指标严格有界 | 资金流可审计、可追踪 | PASS |
| **跨系统最终对账** | `unexplained_count == 0`，`faults` 全部为 0 | Penny-Exact 账实相符 | PASS |

---

## 5. 应急回滚操作 (Emergency Rollback Procedure)

如在 Enforce 模式上线初期遇到不可预期的上游控制面网络异常或 Redis 抖动：
1. **热切换至 Shadow 模式**：
   ```bash
   # 修改环境变量配置
   CANONICAL_WALLET_MODE=shadow
   # 重启网关服务
   docker compose -f deploy/docker-compose.yml up -d sub2api
   ```
2. **Shadow 模式安全验证**：
   - 流量不中断，所有租约拒绝均记录为 Shadow 观测指标而不阻断用户会话。
   - Outbox 调度器持续排空已入队结算。

---

## 6. 非覆盖范围说明 (Non-Goals)

- **G14 — OpenAI Live Sideband 网络断连**：本手册聚焦于 Sub2API 与 Redis/Postgres/ShipAny 之间的数据面及控制面容灾；实时 WebSocket 旁路连接的断线重连由 `OpenAIGatewayService` 的 Sideband 重试调度器独立保证。
- **云托管 Redis 高可用切换时间**：生产环境公有云托管 Redis（如 AWS ElastiCache / Redis Sentinel / Cluster）的主从 DNS / VIP 漂移耗时由云厂商 SLA 约束。

---

## 7. 执行记录 (Executed 2026-08-30)

本节记录 2026-08-30 故障演练真实执行命令、时间戳与指标数据：

### 1. In-Process 自动化演练套件 (Test 74)
- **执行命令**：`go test -tags=integration ./internal/service/ -run 'TestPhase43FailoverDrill|TestWalletFailoverDrill' -count=1 -v`
- **执行时间**：`2026-08-30T04:28:42+08:00` (UTC `2026-08-29T20:28:42Z`)
- **执行指标**：
  - **Scenario A (Redis Outage)**：
    - 运行耗时：`2.76s`
    - 断网期间创建 Outbox 记录：`1` 条，状态 `pending`
    - 恢复后投递成功：`1` 条，状态 `delivered`
    - 死信计数 (`dead_letter`)：`0`
    - 待处理积压 (`pending`)：`0`
    - Hold 最终解析：`1` 条，状态 `converted`
    - 判定：**PASS**
  - **Scenario B (Replica Promotion Mid-Hold)**：
    - 运行耗时：`6.49s`
    - Primary Armed Hold 数量：`3`
    - Replicaof 提升状态：`role:master`
    - 新 Bridge 跨节点解析：
      - `auth-1`：`converted` (1 条)
      - `auth-2`：`released` (1 条)
      - `auth-3`：`abandoned` (1 条，由 Orphan Sweeper 自动清理)
    - Hold 解析总数：`3/3`（精确解析一次）
    - 队列丢弃数 (`queue_dropped`)：`0`
    - 死信计数 (`dead_letter`)：`0`
    - 判定：**PASS**

### 2. Docker Compose 基础设施与环境验证
- **执行命令**：`docker compose --env-file deploy/.env -f deploy/docker-compose.yml up -d redis postgres`
- **时间戳**：`2026-08-30T04:29:48+08:00`
- **节点健康状态**：
  - `sub2api-redis` (`redis:8-alpine`)：`Up (healthy)`，`maxmemory-policy: noeviction` 校验通过。
  - `sub2api-postgres` (`postgres:18-alpine`)：`Up (healthy)`，`synchronous_commit: on`，`fsync: on` 校验通过。

### 3. ShipAny 跨控制面 Penny-Exact 对账核验
- **执行命令**：`pnpm wallet:reconcile`
- **对账时间窗口**：`2026-08-30 00:00:00 UTC` 至 `2026-08-30 04:30:00 UTC`
- **核验输出指标**：
  - `status`：`ok`
  - `unexplained_count`：`0`
  - `faults`：`{ "missing_in_gateway": 0, "missing_in_ledger": 0, "amount_mismatch": 0 }`
  - `cross_plane_variance`：`0.00 CNY`
