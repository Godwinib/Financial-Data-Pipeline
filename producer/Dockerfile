FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer code
COPY producer.py .

# Create logs directory
RUN mkdir -p /app/logs

CMD ["python", "-u", "producer.py"]