# syntax=docker/dockerfile:1.6

FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG REPO_URL=https://github.com/coconutsRhealthy/claude_diski_data.git
ARG GIT_REF=main
ARG CACHE_BUST=0

WORKDIR /app

RUN git clone --depth 1 --branch ${GIT_REF} ${REPO_URL} /app \
    && echo "built from ${GIT_REF} cache=${CACHE_BUST}" > /app/.build-info

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["python", "main.py"]
