FROM eclipse-temurin:17-jdk-jammy

# Install Python (the openjdk base image doesn't include it)
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV INPUT_PATH=/app/data/superstore.csv
ENV OUTPUT_PATH=/app/data/silver_output

# Install Python dependencies first (before copying code) to leverage Docker layer caching
COPY requirements.txt .
RUN pip3 install --no-cache-dir --default-timeout=120 --retries=5 -r requirements.txt

# Copy the script and data
COPY scripts/silver_transform_standalone.py .
COPY data/superstore.csv ./data/superstore.csv

CMD ["python3", "silver_transform_standalone.py"]