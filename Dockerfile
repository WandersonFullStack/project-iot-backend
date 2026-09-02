FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . .

RUN useradd --create-home --uid 10001 appuser

ENV PATH="/app/.venv/bin:${PATH}"

USER appuser

EXPOSE 8000 9000 502

CMD ["sh", "-c", "alembic upgrade head && exec uvicorn app.main.main:app --host 0.0.0.0 --port 8000"]
