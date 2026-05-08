# Image officielle de Python
FROM python:3.11-slim

# 🔥 Recréer des sources Debian stables + installer Java
RUN echo "deb http://deb.debian.org/debian bookworm main" > /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian bookworm-updates main" >> /etc/apt/sources.list && \
    echo "deb http://security.debian.org/debian-security bookworm-security main" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Java pour PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Installer uv
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv
ENV UV_SYSTEM_PYTHON=1
ENV PATH="/code/.venv/bin:$PATH"

# Configuration de l'environnement Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Créer le dossier de travail
WORKDIR /code

# Copier les fichiers projet
COPY pyproject.toml uv.lock ./

# Installer les dépendances
RUN uv sync

# Copier le code
COPY ./scripts/ /code/scripts

CMD ["marimo", "edit", "--no-token", "--host", "0.0.0.0", "-p", "8080"]