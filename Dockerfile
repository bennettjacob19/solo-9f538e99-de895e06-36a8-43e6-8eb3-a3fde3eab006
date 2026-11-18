FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

# A single uvicorn process owns the in-process build workers. Scale with
# `--workers N` or extra replicas only if you want multiple indexers; the advisory
# locks make that safe, but one process is enough for this service.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
