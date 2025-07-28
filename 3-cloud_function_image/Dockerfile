# ---- STAGE 1: Build ----
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends gcc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python packages to a temp dir
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- STAGE 2: Runtime ----
FROM python:3.12-slim

WORKDIR /app

# Copy app code
COPY --from=builder /install /usr/local
COPY dlt_chi_crime_bigquery.py .
COPY bq_creds.json .
COPY requirements.txt .
COPY .env .

RUN mv dlt_chi_crime_bigquery.py main.py

# Install Functions Framework
RUN pip install --no-cache-dir functions-framework

# Required env var for GCP auth
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/bq_creds.json

# Set the port for Cloud Run
ENV PORT=8080

# Specify function target
ENV FUNCTION_TARGET=run_pipeline

# Start the functions framework server
CMD ["functions-framework", "--target=run_pipeline", "--port=8080"]
