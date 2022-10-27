# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /usr/src/app

COPY ./producers-docker_image/requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./producers/* .

CMD [ "python3", "kinesis-publisher.py"]