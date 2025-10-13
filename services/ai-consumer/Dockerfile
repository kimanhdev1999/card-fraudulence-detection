FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py ./

ENV KAFKA_BOOTSTRAP=broker1:9092 \
    GROUP_ID=ai-consumer \
    INPUT_TOPIC=transactions \
    PREDICTIONS_TOPIC=predictions

CMD ["python", "consumer.py"]


