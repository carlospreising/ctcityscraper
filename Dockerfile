# ---- build stage ----
FROM python:3.11-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential python3-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (layer cache)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy source and install the project itself
COPY src/ src/
COPY scrapers/ scrapers/
COPY scrape.py vgsi_cities_ct.json ./
RUN uv sync --frozen --no-dev

# ---- runtime stage ----
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

VOLUME ["/app/data"]

ENTRYPOINT ["scrape"]
CMD ["--help"]
