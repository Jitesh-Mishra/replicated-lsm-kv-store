FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir fastapi==0.162.0 uvicorn==0.54.0 httpx==0.28.1 prometheus-client==0.26.0
COPY lsmkv ./lsmkv
ENV DATA_DIR=/data FSYNC=always
VOLUME /data
EXPOSE 8080
CMD ["uvicorn", "lsmkv.service.server:app", "--host", "0.0.0.0", "--port", "8080"]
