# Image de base Python
FROM python:3.11-slim

# Installer dépendances système (optionnel mais utile pour dbt + postgres)
RUN apt-get update && \
    apt-get install -y \
        git \
        curl \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Installer uv (OBLIGATOIRE pour uv sync)
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv

# Variables d'environnement uv
ENV UV_SYSTEM_PYTHON=1

# Dossier de travail
WORKDIR /dbt

# Copier fichiers de dépendances
COPY pyproject.toml uv.lock ./

# Installer les dépendances
RUN uv sync

# Ajouter le venv au PATH
ENV PATH="/dbt/.venv/bin:$PATH"

# Copier le projet dbt
COPY . /dbt

# Commande par défaut (container idle)
CMD ["tail", "-f", "/dev/null"]