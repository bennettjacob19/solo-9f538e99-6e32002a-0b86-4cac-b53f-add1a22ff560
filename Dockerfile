FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

EXPOSE 8000

# 默认启动演练控制台; 副本服务由 docker-compose 覆盖 command 启动
CMD ["uvicorn", "drill_app:app", "--host", "0.0.0.0", "--port", "8000"]
