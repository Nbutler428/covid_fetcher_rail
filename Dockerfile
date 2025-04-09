FROM python:3.11-alpine

# Install dependencies
RUN apk add --no-cache gcc musl-dev libpq postgresql-dev

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
COPY app.py .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Run app
CMD ["python", "app.py"]
