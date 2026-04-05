# EUR-LHD Scraper — production Docker image
# Designed for DigitalOcean App Platform (Worker type)
#
# Always runs headed Chrome via Xvfb virtual display.
# Mirrors the setup of our main RealOEM scraper exactly.

FROM python:3.13-slim

# System dependencies for headed Chromium + Xvfb virtual display
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    xauth \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2-dev \
    libpangocairo-1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright: install Chromium + all OS-level deps in one command (same as main scraper)
RUN playwright install chromium --with-deps

# Project source
COPY . .
RUN mkdir -p /app/output /app/test-data

# Runtime defaults — override via DO App Platform env vars
ENV PYTHONUNBUFFERED=1
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
