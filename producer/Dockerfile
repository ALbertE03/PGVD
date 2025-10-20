FROM python:3.11-slim

WORKDIR /app

# IMPORTANTE: Copiar requirements.txt primero (se cachea si no cambia)
COPY requirements.txt .

# Instalar dependencias de Python (se cachea si requirements.txt no cambia)
RUN pip install --no-cache-dir -r requirements.txt

# Copiar archivos de código Python
COPY producer.py .
COPY config.py .
COPY family_generator.py .
COPY streaming_manager.py .
COPY aed.ipynb .

# Crear directorios necesarios
RUN mkdir -p /app/data /app/family /app/models

# Copiar datos y módulos (esto va al final para que cambios en código no invaliden cache de pip)
COPY data/ /app/data/
COPY family/ /app/family/
COPY models/ /app/models/

CMD ["python", "producer.py"]