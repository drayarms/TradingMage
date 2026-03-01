FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . . # Copy the rest of the contents of src parent dir to docker container's /app

ENV PYTHONUNBUFFERED=1 
EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"] #Start the Uvicorn server, load the FastAPI app object named app from app.py, listen on all network interfaces, and accept traffic on port 8000.