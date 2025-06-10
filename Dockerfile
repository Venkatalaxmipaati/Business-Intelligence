# ──────────────────────────────────────────────────────────────────────────────
# Dockerfile
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# 1) Set working directory
WORKDIR /app

# 2) Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3) Copy the entire project into /app
COPY . .

# 4) Run the ETL script under its actual name
CMD ["python", "app/etl_weather.py.py"]
