# Packaging stage only. The release workflow cross-compiles both architectures
# natively on the runner and this image merely ships the resulting binaries.
# For a local image build run `cargo build --release` first and place the
# binary in dist/amd64 (or dist/arm64).

FROM debian:bookworm-slim

# amd64 or arm64, selected by buildx for each platform.
ARG TARGETARCH

ENV LLM_PROXY_HOST=0.0.0.0 \
    LLM_PROXY_PORT=8080 \
    LLM_PROXY_DATA_DIR=/data

# Keep the data directory writable by the runtime UID without running a
# target-architecture command during the image build.
COPY --chown=10001:10001 docker/data/.gitkeep /data/.gitkeep
COPY dist/${TARGETARCH}/llm-proxy /usr/local/bin/llm-proxy

USER 10001:10001
EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["llm-proxy"]
