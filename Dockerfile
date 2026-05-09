FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mqtt_broker/ ./mqtt_broker/
COPY config.yaml .

EXPOSE 1883

CMD ["python", "-m", "mqtt_broker", "--config", "config.yaml"]
