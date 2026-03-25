FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

# Clonar el repositorio completo (rama por defecto)
ARG REPO_URL=https://github.com/eidersantamariaa/Data-Lakehouse.git

RUN git clone ${REPO_URL} /app

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Exponer puerto
EXPOSE 8000

# Comando para correr solo el UI
CMD ["uvicorn", "ui:app", "--host", "0.0.0.0", "--port", "8000"]
