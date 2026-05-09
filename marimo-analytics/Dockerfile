FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# uv uniquement
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv

ENV UV_SYSTEM_PYTHON=1
ENV PATH="/code/.venv/bin:$PATH"

WORKDIR /code

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY ./scripts/ /code/scripts

CMD ["marimo", "edit", "--no-token", "--host", "0.0.0.0", "-p", "8080"]