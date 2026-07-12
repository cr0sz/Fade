FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY signal_edge_analyzer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy module source
COPY signal_edge_analyzer/ .
COPY paper_trading_engine/meta_strategy.py /app/meta_strategy.py

# Copy migration SQL so db.py can apply it on startup
COPY migrations/013_signal_edge_analyzer.sql /migrations/013_signal_edge_analyzer.sql

ENV PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

EXPOSE 8022

CMD ["python", "main.py"]
