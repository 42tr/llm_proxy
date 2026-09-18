FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LLM_PROXY_HOST=0.0.0.0 \
    LLM_PROXY_PORT=8080 \
    LLM_PROXY_DATA_DIR=/data

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt

COPY app.py storage.py README.md ./
COPY static ./static

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin llmproxy \
    && mkdir -p /data \
    && chown -R llmproxy:llmproxy /app /data

USER llmproxy
EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["python3", "app.py"]
