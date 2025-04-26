# Use a stable Python base image (bullseye, not bookworm)
FROM --platform=linux/amd64 python:3.10-bullseye

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Riga


WORKDIR /app

# Clean up trusted.gpg.d (optional, for broken GPG issues)
RUN rm -rf /etc/apt/trusted.gpg.d/*

# Install system dependencies and set timezone
RUN apt-get update && \
    apt-get install -y wget curl unzip gnupg xvfb tzdata libnss3 libx11-6 libatk-bridge2.0-0 libatspi2.0-0 libgtk-3-0 libxcomposite1 libxcursor1 libxdamage1 libxrandr2 libgbm-dev fonts-liberation libasound2 libdrm2 libxkbcommon0 libxfixes3 libcups2 libnspr4 libu2f-udev libwayland-client0 && \
    ln -snf /usr/share/zoneinfo/Europe/Riga /etc/localtime && echo "Europe/Riga" > /etc/timezone && \
    rm -rf /var/lib/apt/lists/*

# Add Google Chrome repository and install Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | \
    gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/google-chrome
RUN ln -s /usr/bin/google-chrome /usr/bin/chromium || true

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose no ports (Telegram bot is outbound only)

# Default command to run the Telegram bot
CMD ["python", "amazon_b.py"] 