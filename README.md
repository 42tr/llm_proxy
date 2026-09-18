# LLM Proxy

这是一个单进程的 OpenAI 兼容 `chat/completions` 代理。它不依赖 Redis、Kafka、PostgreSQL 等外部服务；上游服务和模型映射通过浏览器管理页面配置，配置保存到本地 SQLite，客户端只需要传 `model`。

## 安装和启动

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python3 app.py --host 127.0.0.1 --port 8080 --data-dir ./data
```

打开 `http://127.0.0.1:8080/admin`。未设置密钥环境变量时，首次启动会在 `data/admin.token` 和 `data/client.token` 生成随机 Token，并打印文件位置。

生产环境建议显式设置：

```bash
export LLM_PROXY_ADMIN_KEY='admin-secret'
export LLM_PROXY_API_KEY='client-secret'
export LLM_PROXY_MASTER_KEY='a-long-local-secret'
```

`LLM_PROXY_MASTER_KEY` 用于加密 SQLite 中的上游 API Key；更换它会导致已有密钥无法解密，需要重新录入上游密钥。

## 配置流程

1. 在“上游服务”中填写完整的 `chat/completions` 地址、认证方式、API Key、额外请求头和超时。
2. 在“模型映射”中将客户端模型名映射到上游服务和上游模型名。
3. 保存后立即生效，不需要重启进程。

客户端不需要知道上游地址，也不需要传路由 ID：

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'Authorization: Bearer client-secret' \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

`stream=true` 时，SSE 数据会边读边转发；非流式响应会转发上游状态码和正文。调用记录按天写到 `data/logs/YYYY-MM-DD.jsonl`，可分别控制请求和响应正文是否入日志，正文有大小上限。

## 管理 API

```text
GET/POST       /api/admin/providers
GET/PUT/DELETE /api/admin/providers/{id}
POST           /api/admin/providers/{id}/test
GET/POST       /api/admin/model-routes
GET/PUT/DELETE /api/admin/model-routes/{id}
GET            /api/admin/logs?date=YYYY-MM-DD&q=keyword&limit=100
GET            /api/admin/access
GET            /v1/models
GET            /healthz
```

上游服务需要兼容 OpenAI `chat/completions` 请求和 SSE 响应格式。认证和管理接口应放在 HTTPS 或受保护的内网中；SQLite、密钥文件和日志目录会尝试设置为仅当前用户可读写。

## Docker 部署

镜像提供 `linux/amd64` 和 `linux/arm64` 两种架构，发布位置与 `diting` 一致：

- `ghcr.io/42tr/llm_proxy:<version>`
- `crpi-gz6f3ok0ezphywc8.cn-shanghai.personal.cr.aliyuncs.com/42tr/llm_proxy:<version>`

两处同时更新 `latest`。例如 tag `v0.1.0` 对应镜像标签 `0.1.0`。

```bash
docker run -d --name llm-proxy \
  -p 127.0.0.1:8080:8080 \
  -v llm-proxy-data:/data \
  ghcr.io/42tr/llm_proxy:0.1.0

# 读取首次启动自动生成的管理 Token，填写到管理页面
docker exec llm-proxy cat /data/admin.token
```

镜像使用 UID 10001 的非 root 用户。`/data` 必须持久化，它保存 SQLite、加密主密钥、访问 Token 和调用日志。默认使用 Docker 命名卷；使用宿主机目录挂载时需要给 UID 10001 写权限。不要提交或公开这些数据文件。

可通过 `-e LLM_PROXY_ADMIN_KEY`、`-e LLM_PROXY_API_KEY`、`-e LLM_PROXY_MASTER_KEY` 注入环境中已有的密钥。镜像默认监听 `0.0.0.0:8080`，支持 `LLM_PROXY_HOST`、`LLM_PROXY_PORT`、`LLM_PROXY_DATA_DIR` 环境变量，命令行参数优先。

## 发布

`.github/workflows/release.yml` 在推送 tag 时执行集成测试，然后通过 QEMU/Buildx 构建并推送多架构镜像。GHCR 使用工作流的 `GITHUB_TOKEN`；阿里云使用仓库 Secrets `ALIYUN_REGISTRY_USERNAME` 和 `ALIYUN_REGISTRY_PASSWORD`。沿用 `diting` 设置，关闭 provenance 以兼容阿里云 ACR 个人版。

```bash
git push origin main
git tag -a v0.1.0 -m 'Release v0.1.0'
git push origin v0.1.0
```
