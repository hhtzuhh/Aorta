# Use Python 3.11 (Cloud Run supports up to 3.11)
FROM python:3.11-slim

# Install system dependencies + Google Cloud SDK
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

# Set working directory and PYTHONPATH
WORKDIR /app
ENV PYTHONPATH=/app

# Copy pyproject.toml and install dependencies
COPY pyproject.toml /app/Aorta/
WORKDIR /app/Aorta
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Copy application code into Aorta subdirectory
COPY . /app/Aorta/

# Create _data directory for downloaded database
RUN mkdir -p _data

# Create startup script
RUN echo '#!/bin/bash\n\
set -e\n\
echo "Downloading SQLite database from Cloud Storage..."\n\
if [ -n "$DB_BUCKET" ]; then\n\
  gcloud storage cp gs://${DB_BUCKET}/mimic_demo.db /app/Aorta/_data/mimic_demo.db\n\
  echo "✅ Database downloaded successfully"\n\
else\n\
  echo "⚠️  Warning: DB_BUCKET not set, skipping database download"\n\
fi\n\
echo "📂 Checking ML models..."\n\
ls -la /app/Aorta/ml/models/local/ 2>&1 || echo "❌ ML models directory not found"\n\
echo "Starting Aorta backend..."\n\
exec uvicorn backend.api.main:app --host 0.0.0.0 --port ${PORT:-8080}\n\
' > /app/Aorta/start.sh && chmod +x /app/Aorta/start.sh

# Expose port
EXPOSE 8080

# Run startup script
CMD ["/app/Aorta/start.sh"]
