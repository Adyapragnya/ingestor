# Dockerfile
FROM python:3.11-slim

# System deps (optional but often useful: curl, nano, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Environment for Python
ENV PYTHONUNBUFFERED=1

# Expose internal port
EXPOSE 8000

# IMPORTANT: keep workers = 1, because each process will start
# its own AIS worker + merge worker. We want exactly one ingestor.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
