# Packaging stage only. The release workflow cross-compiles both architectures
# natively on the runner and this image merely ships the resulting binaries,
# which keeps multi-architecture builds out of slow QEMU Rust compilation.
# For a local image build run `cargo build --release` first and place the
# binary in dist/amd64 (or dist/arm64).

FROM debian:bookworm-slim

# amd64 or arm64, selected by buildx for each platform.
ARG TARGETARCH

ENV LLM_PROXY_HOST=0.0.0.0 \
    LLM_PROXY_PORT=8080 \
    LLM_PROXY_DATA_DIR=/data

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin llmproxy \
    && mkdir -p /data \
    && chown llmproxy:llmproxy /data

COPY dist/${TARGETARCH}/llm-proxy /usr/local/bin/llm-proxy

USER llmproxy
EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["llm-proxy"]
