FROM python:3.11.9-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/sbosc

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \ 
    default-libmysqlclient-dev \
    default-mysql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/sbosc

# Install any needed packages specified in requirements.txt
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./
