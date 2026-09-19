# LLM Proxy

这是一个用 Rust 实现的单进程 OpenAI 兼容 `chat/completions` 代理。它不依赖 Redis、Kafka、PostgreSQL 等外部服务；上游服务和模型映射通过浏览器管理页面配置，配置保存到本地 SQLite，客户端只需要传 `model`。

## 安装和启动

```bash
cargo build --release
./target/release/llm-proxy --host 127.0.0.1 --port 8080 --data-dir ./data
```

需要 Rust 1.85 及以上版本。SQLite 由 `rusqlite` 的 `bundled` 特性随构建一起编译，因此构建机需要 C 编译器和链接器；运行阶段没有额外依赖。管理页面通过 `include_str!` 编译进二进制，部署时只需要一个可执行文件加数据目录。修改 `static/` 下的前端文件后必须重新编译才会生效。

开发期可以直接 `cargo run -- --host 127.0.0.1 --port 8080 --data-dir ./data`。测试包含 Fernet 加密的单元测试和覆盖代理、管理接口、日志查询的端到端集成测试：

```bash
cargo test
```

`Cargo.lock` 应随仓库一起提交以保证可复现构建；升级依赖后请一并提交新的锁文件。

打开 `http://127.0.0.1:8080/admin`。未设置密钥环境变量时，首次启动会在 `data/admin.token` 和 `data/client.token` 生成随机 Token，并打印文件位置。

管理页面首屏是登录框：输入管理 API Key 并用 `/api/admin/access` 验证通过后才会显示配置界面。验证通过的 Key 保存在浏览器 `localStorage`（键名 `llm-proxy.adminKey`），下次打开自动登录，无需重复输入；Key 输错、服务端轮换或任意管理接口返回 401 时会清除本地保存并退回登录框。页面右上角“退出登录”可主动清除。共用电脑建议登录后手动退出。

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

上游服务和模型映射的表单只在点击“新增/编辑”时以弹框形式出现；“调用日志”的查看弹框会直接展示格式化后的入参（客户端请求正文）与出参（上游响应正文），附带字节数、编码和截断提示，未开启正文记录时显示原因，底部仍保留完整日志 JSON 供排查。

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

`.github/workflows/release.yml` 在推送 tag 时先用 Rust 工具链执行 `cargo fmt --all --check`、`cargo clippy --all-targets -- -D warnings` 和 `cargo test`，再通过 QEMU/Buildx 构建并推送多架构镜像。GHCR 使用工作流的 `GITHUB_TOKEN`；阿里云使用仓库 Secrets `ALIYUN_REGISTRY_USERNAME` 和 `ALIYUN_REGISTRY_PASSWORD`。沿用 `diting` 设置，关闭 provenance 以兼容阿里云 ACR 个人版。

改用 Rust 之后，`linux/arm64` 镜像需要在 QEMU 模拟下原生编译整棵依赖树（含 bundled SQLite），构建时间比原来的 Python 打包长很多；工作流里的 `Swatinem/rust-cache` 只加速宿主机架构上的 `clippy`/`test` 步骤，跨架构构建缓存不共享。如需缩短发布时间，可以改用交叉编译目标或原生 arm64 runner。

```bash
git push origin main
git tag -a v0.1.0 -m 'Release v0.1.0'
git push origin v0.1.0
```
