FROM python:3.13-slim
WORKDIR /app
COPY tisseo_live.py index.html manifest.json sw.js icon.svg ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "tisseo_live.py"]
