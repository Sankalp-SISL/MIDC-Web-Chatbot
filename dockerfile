FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (for PDF handling stability)
RUN apt-get update && apt-get install -y \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV PORT=8080

CMD ["python", "main.py"]
