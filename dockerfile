# Dockerfile

FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the Python script
COPY extract_data.py .

# Run the extraction script
CMD ["python", "extract_data.py"]
