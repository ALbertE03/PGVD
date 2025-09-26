FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY conf.py .
COPY const.py .
COPY genomic_generator.py .
COPY aed.ipynb .
RUN mkdir -p /app/data

COPY data/ /app/data/

EXPOSE 8080 8501

CMD ["python", "producer.py"]