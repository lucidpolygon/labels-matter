FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

WORKDIR /app
COPY 1-lexis-collect-cases-v2.py /app/lexis-collect-cases-v2.py
RUN pip install --no-cache-dir playwright python-dotenv requests

CMD ["python", "/app/lexis-collect-cases-v2.py"]
