FROM python:3.10-slim
WORKDIR /app
# Install ffmpeg and clean up in one layer to keep image size small
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy handler code
COPY handler.py .
# Run with unbuffered output
CMD ["python", "-u", "handler.py"]
