FROM python:3.12.1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Instala Gunicorn
RUN pip install gunicorn

# Define el puerto directamente en el comando CMD
CMD ["gunicorn", "-b", "0.0.0.0:9115", "app:app"]
