FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivos de proyecto
COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY tests/ ./tests/

# Instalar poetry
RUN pip install --no-cache-dir poetry

# Configurar poetry para no crear entorno virtual dentro del contenedor
RUN poetry config virtualenvs.create false

# Instalar dependencias
RUN poetry install --no-dev

# Crear directorio para logs
RUN mkdir -p /app/logs

# Variables de entorno por defecto
ENV LOG_LEVEL=INFO
ENV API_PORT=8000

# Exponer puerto para health-check
EXPOSE 8000

# Comando para ejecutar la aplicación
CMD ["python", "-m", "src.main"]
