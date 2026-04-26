FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt

COPY . .

ENV HOST=0.0.0.0 \
    PORT=8010

CMD ["sh", "-c", "HOST=${HOST:-0.0.0.0} PORT=${PORT:-8010} make serve"]
