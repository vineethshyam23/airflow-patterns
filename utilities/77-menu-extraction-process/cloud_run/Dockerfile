FROM mcr.microsoft.com/playwright/python:v1.60.0-jammy

WORKDIR /app
COPY app.py .
RUN pip install --no-cache-dir \
    flask \
    gunicorn \
    playwright==1.60.0 \
    playwright-stealth

ENV PORT=8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "300", "--workers", "1", "app:app"]
