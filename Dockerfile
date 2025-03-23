FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p data logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Entry script
COPY railway_entrypoint.sh .
RUN chmod +x railway_entrypoint.sh

CMD ["./railway_entrypoint.sh"]