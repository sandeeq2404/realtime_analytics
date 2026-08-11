FROM python:3.11-slim

LABEL maintainer="FoodStream Analytics"
LABEL description="Kafka producer that generates real-time food order events"

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY order_producer.py .

CMD ["python", "-u", "order_producer.py"]
