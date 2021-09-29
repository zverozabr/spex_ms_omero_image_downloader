FROM python:3.9.2
USER root

ENV PYTHONDONTWRITEBYTECODE = 1
ENV PYTHONUNBUFFERED = 1

COPY ./microservices/ms-omero-image-downloader /app/services/app
COPY ./common /app/common

WORKDIR /app/services/app

RUN pip install pipenv && pipenv install --system --deploy --ignore-pipfile


CMD ["python", "app.py"]
