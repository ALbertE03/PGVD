FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY conf.py .
COPY family_generator.py .
COPY aed.ipynb .
RUN mkdir -p /app/data
RUN mkdir -p /app/family

COPY data/ /app/data/
COPY family/ /app/family/

EXPOSE 8080 8501

CMD ["python", "producer.py"]