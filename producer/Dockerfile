FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy Python source files
COPY producer.py .
COPY config.py .
COPY family_generator.py .
COPY streaming_manager.py .
COPY aed.ipynb .

# Create necessary directories
RUN mkdir -p /app/data
RUN mkdir -p /app/family
RUN mkdir -p /app/models

# Copy data and modules
COPY data/ /app/data/
COPY family/ /app/family/
COPY models/ /app/models/

EXPOSE 8080 8501

CMD ["python", "producer.py"]