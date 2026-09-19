# syntax=docker/dockerfile:1

# Build stage. The slim Rust image already ships gcc and libc-dev, which the bundled
# SQLite build needs; the admin console is compiled into the binary via include_str!.
ARG RUST_VERSION=1
FROM rust:${RUST_VERSION}-slim-bookworm AS build

WORKDIR /build

# Copy the manifest (and the lock file when it is committed) first to keep layer cache hits.
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY static ./static

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release && cp target/release/llm-proxy /build/llm-proxy

FROM debian:bookworm-slim

ENV LLM_PROXY_HOST=0.0.0.0 \
    LLM_PROXY_PORT=8080 \
    LLM_PROXY_DATA_DIR=/data

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin llmproxy \
    && mkdir -p /data \
    && chown llmproxy:llmproxy /data

COPY --from=build /build/llm-proxy /usr/local/bin/llm-proxy

USER llmproxy
EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["llm-proxy"]
