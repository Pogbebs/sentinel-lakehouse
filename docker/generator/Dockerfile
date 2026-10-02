FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /opt/sentinel
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[generator]"

USER nobody
ENTRYPOINT []
