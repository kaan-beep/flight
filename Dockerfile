FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY opensky_client.py db.py producer.py consumer_db.py ws_server.py ./

# Gerçek komut docker-compose.yml içinde her servis için ayrı verilir.
CMD ["python", "producer.py"]
