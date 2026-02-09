FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY 1-lexis-collect-cases-v2.py /app/1-lexis-collect-cases-v2.py
COPY 2-lexis-download-complaint.py /app/2-lexis-download-complaint.py
COPY 3-prop65.py /app/3-prop65.py
COPY 4-prop65-generate-content.py /app/4-prop65-generate-content.py
COPY 5-lexis-generate-content.py /app/5-lexis-generate-content.py
COPY 6-publish-to-website.py /app/6-publish-to-website.py

RUN pip install --no-cache-dir playwright python-dotenv requests boto3 openai

# default (can be overridden by Render Start Command)
CMD ["python", "/app/1-lexis-collect-cases-v2.py"]
