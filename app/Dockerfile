FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=15 \
    PIP_RETRIES=1

WORKDIR /app

COPY requirements.txt .

# Primary index first; some networks block the Python CDN, so fall back to a
# public mirror. Override with --build-arg PIP_INDEX_URL=...
ARG PIP_INDEX_URL=""
RUN if [ -n "$PIP_INDEX_URL" ]; then \
        pip install --no-cache-dir --index-url "$PIP_INDEX_URL" -r requirements.txt; \
    else \
        pip install --no-cache-dir -r requirements.txt \
        || pip install --no-cache-dir --index-url https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com -r requirements.txt; \
    fi

COPY . .

CMD ["python", "-c", "print('payment-data-platform app image'); import time; time.sleep(3600)"]
