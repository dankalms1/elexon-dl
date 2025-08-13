FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1

WORKDIR /app
COPY . /app
RUN python -m pip install -U pip &&     pip install .[parquet]

ENTRYPOINT ["elexon-dl"]
