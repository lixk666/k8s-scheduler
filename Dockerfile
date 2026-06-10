FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_aware_scheduler ./load_aware_scheduler

USER 65532:65532
ENTRYPOINT ["python", "-m", "load_aware_scheduler"]
