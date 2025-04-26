# Use a stable Python base image
FROM --platform=linux/amd64 python:3.10-slim

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Riga

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y wget curl unzip gnupg xvfb libnss3 libx11-6 libatk-bridge2.0-0 libatspi2.0-0 libgtk-3-0 libxcomposite1 libxcursor1 libxdamage1 libxrandr2 libgbm-dev fonts-liberation libasound2 libdrm2 libxkbcommon0 libxfixes3 libcups2 libnspr4 libu2f-udev libwayland-client0 && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    rm -rf /var/lib/apt/lists/*

# Download and install Google Chrome directly
RUN wget -O /tmp/google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get update && \
    apt-get install -y /tmp/google-chrome.deb && \
    rm /tmp/google-chrome.deb && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose no ports (Telegram bot is outbound only)

# Default command to run the Telegram bot
CMD ["python", "amazon_b.py"] 