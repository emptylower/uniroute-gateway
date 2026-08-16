# UniRoute Gateway + ShipAny 部署手册

本文档适用于本仓库的 ShipAny 数据平面版本。ShipAny 负责账号、权限、支付、积分和 API Key 生命周期；本服务只负责模型流量、上游账号、路由、流式连接、Redis 状态和使用量记录。

## 重要状态

- `SERVER_DATA_PLANE_ONLY=true` 会在路由注册阶段移除旧登录、用户、支付、Webhook、管理面板和嵌入前端入口。
- ShipAny 与网关之间使用独立的短期签名，不复用登录 JWT。
- 当前钱包桥只支持 `disabled` 和观测用 `shadow`。`enforce` 会被配置校验拒绝，不能用于正式收费授权。
- 联调时可以使用 `RUN_MODE=simple`，但它会跳过网关旧余额检查。正式收费流量必须等权威钱包的预留、结算、退款和持久重试完成后再切换。

## 1. 服务器准备

建议使用 Ubuntu 24.04、4 核 CPU、8 GB 内存、100 GB SSD。安装 Docker Engine、Compose 插件、Git 和 OpenSSL，并准备一个 HTTPS 域名，例如 `gateway.example.com`。

只对公网开放 `80/443`。PostgreSQL、Redis 不映射到宿主机；网关默认只监听 `127.0.0.1:8080`，由 Nginx 或 Caddy 终止 TLS。

## 2. 获取源码

```bash
git clone https://github.com/emptylower/uniroute-gateway.git
cd uniroute-gateway
cp deploy/.env.shipany.example deploy/.env.shipany
chmod 600 deploy/.env.shipany
```

## 3. 生成服务器密钥

在服务器终端分别运行多次 `openssl rand -hex 32`，把不同结果写入 `deploy/.env.shipany`：

- `POSTGRES_PASSWORD`
- `REDIS_PASSWORD`
- `JWT_SECRET`
- `TOTP_ENCRYPTION_KEY`
- `PLATFORM_IDENTITY_SECRET`

这些值必须彼此独立。不要通过聊天、工单或仓库传递真实密钥。

首次部署保持：

```env
RUN_MODE=simple
CANONICAL_WALLET_MODE=disabled
```

这只用于联调，不代表收费链路已经切流。

## 4. 构建并启动

本仓库必须从源码构建。不要把 Compose 中的镜像改回上游 `weishaw/sub2api:latest`，否则不会包含 ShipAny 身份和投影功能。

```bash
docker compose \
  --env-file deploy/.env.shipany \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.shipany.yml \
  up -d --build
```

首次启动会初始化 PostgreSQL、Redis 并执行网关 SQL 迁移。检查状态：

```bash
docker compose \
  --env-file deploy/.env.shipany \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.shipany.yml ps

curl -fsS http://127.0.0.1:8080/health
```

查看日志：

```bash
docker compose \
  --env-file deploy/.env.shipany \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.shipany.yml logs -f --tail=200 sub2api
```

## 5. 配置 HTTPS 反向代理

Nginx 示例：

```nginx
server {
    listen 443 ssl http2;
    server_name gateway.example.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_buffering off;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
```

生产环境需要配置可信代理范围后才能启用转发 IP 信任；不要无条件信任客户端提交的 `X-Forwarded-For`。具体规则见 [EDGE_SECURITY.md](./EDGE_SECURITY.md)。

## 6. ShipAny / Cloudflare 对接

网关启动且 HTTPS 可访问后，在 ShipAny Worker 中设置：

| ShipAny 配置 | 值 |
| --- | --- |
| `ROUTER_INTERNAL_API_URL` | `https://gateway.example.com` |
| `ROUTER_CONTROL_API_URL` | `https://gateway.example.com/api/v1` |
| `ROUTER_SERVICE_IDENTITY_ENABLED` | `true` |
| `ROUTER_SERVICE_IDENTITY_ISSUER` | `shipany` |
| `ROUTER_SERVICE_IDENTITY_AUDIENCE` | `sub2api-gateway` |
| `ROUTER_SERVICE_IDENTITY_VERSION` | `v1` |
| Worker secret `ROUTER_SERVICE_IDENTITY_SECRET` | 与网关 `PLATFORM_IDENTITY_SECRET` 相同 |

密钥配置完成后，ShipAny 的账号、分组、渠道、模型、用量和 API Key 管理会通过签名内部接口访问网关。网关旧管理面板不会重新开放。

## 7. 影子钱包联调

先创建一个新的独立密钥，在 ShipAny 设置 Worker secret `GATEWAY_WALLET_ASSERTION_SECRET`，并在网关设置同值的 `CANONICAL_WALLET_SECRET`。然后将双方模式改为 `shadow`：

```env
# Gateway
CANONICAL_WALLET_MODE=shadow
CANONICAL_WALLET_CONTROL_PLANE_URL=https://all-model-router.app
```

```text
# ShipAny Worker var
GATEWAY_WALLET_MODE=shadow
```

影子模式只记录和对账，不控制请求放行。出现队列丢弃、非零账差或 API Key `pending/failed` 时，必须保持该模式，不能切正式流量。

## 8. 联调验收

至少验证以下流程：

1. ShipAny 用户注册、登录和禁用。
2. ShipAny 创建 API Key，网关投影状态变为 `synced`。
3. 使用该 Key 调用 `/v1/models` 和一次流式模型请求。
4. ShipAny 后台可以创建、测试上游账号并配置渠道和分组。
5. 撤销 API Key 后，网关在目标时间内拒绝旧 Key。
6. 使用量能从网关回读到 ShipAny。
7. 影子结算事件无重复、无跨用户串账，累计金额可对齐。

## 9. 更新、备份与回滚

更新前先备份 PostgreSQL：

```bash
docker compose \
  --env-file deploy/.env.shipany \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.shipany.yml \
  exec -T postgres pg_dump -U sub2api -d sub2api -Fc > sub2api-backup.dump
```

更新：

```bash
git pull --ff-only
docker compose \
  --env-file deploy/.env.shipany \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.shipany.yml \
  up -d --build
```

如需回滚应用版本，切回已知正常的提交并重新构建。数据库迁移是前向执行，涉及数据库结构的回滚必须先恢复备份，不能只回滚容器镜像。

## 10. 正式切流门槛

满足以下条件前，保持联调状态：

- 权威钱包已实现持久的 reserve/capture/refund 和重试机制。
- 影子对账在约定观察期内持续零差异。
- API Key 投影队列中 `pending/failed` 为零。
- 已完成压力测试、Redis 故障测试、数据库备份恢复演练。
- ShipAny 与网关的生产密钥已独立生成并完成轮换演练。

当前版本主动拒绝 `CANONICAL_WALLET_MODE=enforce`，这是安全边界，不应绕过。
