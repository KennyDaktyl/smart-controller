FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY smart_common/requirements.txt /app/smart_common/requirements.txt
RUN pip install --no-cache-dir -r /app/smart_common/requirements.txt

COPY app /app/app
COPY smart_common /app/smart_common
COPY __init__.py /app/__init__.py

CMD ["python", "-m", "app.main"]
