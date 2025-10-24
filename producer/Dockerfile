FROM python:3.11-slim

WORKDIR /app


COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY config.py .
COPY family_generator.py .
COPY streaming_manager.py .
COPY aed.ipynb .

RUN mkdir -p /app/data /app/family /app/models

COPY data/ /app/data/
COPY family/ /app/family/
COPY models/ /app/models/

CMD ["python", "producer.py"]