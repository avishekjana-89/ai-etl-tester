FROM python:3.11-slim

WORKDIR /app

# Ensure Python output is not buffered so logs appear in real-time
ENV PYTHONUNBUFFERED=1

# Install system dependencies (postgresql-client provides pg_isready for entrypoint wait logic)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make the entrypoint executable
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Expose port
EXPOSE 8000

# Start via entrypoint (runs migrations, then uvicorn)
CMD ["/app/entrypoint.sh"]
