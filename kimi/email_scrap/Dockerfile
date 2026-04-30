# Use official Playwright Python image which includes all browser system dependencies
FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Workdir inside container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Verify Playwright installation/bins match the library version 
# (installs chromium by default to save space, remove chromium to install all)
RUN playwright install chromium

# Copy source code
COPY . .

EXPOSE 8067

# Run Uvicorn server on port 8067
CMD ["sh", "-c", "rm -rf /app/app/models && uvicorn app.main:app --host 0.0.0.0 --port 8067 --reload"]


