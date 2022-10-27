# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /usr/src/app
ENV AWS_DEFAULT_REGION us-east-1

COPY ./producers-docker_image/requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./producers/ .
RUN ls -la /*

CMD [ "python3", "kinesis-publisher.py"]