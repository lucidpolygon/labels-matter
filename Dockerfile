FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

WORKDIR /app

COPY 1-lexis-collect-cases-v2.py /app/1-lexis-collect-cases-v2.py
COPY 2-lexis-download-complaint.py /app/2-lexis-download-complaint.py

RUN pip install --no-cache-dir playwright python-dotenv requests boto3

# default (can be overridden by Render Start Command)
CMD ["python", "/app/lexis-collect-cases-v2.py"]
