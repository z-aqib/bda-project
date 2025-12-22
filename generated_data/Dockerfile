FROM python:3.10-slim

# Set working directory inside container
WORKDIR /app

# Copy dependency file and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy generator code
COPY . .

# Run producer when container starts
CMD ["python", "producer.py"]
