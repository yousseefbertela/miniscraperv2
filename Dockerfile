# EUR-LHD Scraper — production Docker image
# Designed for DigitalOcean App Platform (Worker type)
#
# Build:     docker build -t eur-lhd-scraper .
# Run:       docker run --env-file .env eur-lhd-scraper
# DO deploy: set SCRAPER_MODE=current or classic in App Platform env vars

FROM python:3.11-slim

# System dependencies required by Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget gnupg \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 \
    libcairo2 libcups2 libdbus-1-3 libdrm2 libexpat1 \
    libfontconfig1 libgbm1 libgcc-s1 libglib2.0-0 \
    libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 libpangocairo-1.0-0 \
    libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcb-dri3-0 \
    libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 \
    libxi6 libxkbcommon0 libxrandr2 libxrender1 libxss1 libxtst6 \
    fonts-liberation fonts-noto-color-emoji \
    xdg-utils libu2f-udev libvulkan1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright: install Chromium browser + OS-level deps
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN playwright install chromium
RUN playwright install-deps chromium

# Project source
COPY . .
RUN mkdir -p /app/output /app/test-data

# Runtime defaults — override via DO App Platform env vars
ENV PYTHONUNBUFFERED=1
ENV HEADLESS=true
ENV SCRAPER_MODE=current
ENV TEST_MODE=false
ENV DB_HOST=""
ENV DB_PORT=25060
ENV DB_NAME=defaultdb
ENV DB_USER=""
ENV DB_PASSWORD=""
ENV DB_SSLMODE=require

# SCRAPER_MODE controls which entry point runs
CMD ["sh", "-c", "python main_${SCRAPER_MODE}.py"]
