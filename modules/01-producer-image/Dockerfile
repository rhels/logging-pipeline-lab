FROM python:3.12-slim
WORKDIR /app
COPY producer.py /app/producer.py
ENV LINES_PER_SEC=10 LINE_BYTES=1024 TOTAL_LINES=0
ENTRYPOINT ["python3", "-u", "/app/producer.py"]
