# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /usr/src/app

COPY ./application-docker-image/requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./application/ .
RUN ls -la /*

EXPOSE 8501

CMD [ "python3", "-m", "streamlit", "run", "Home.py"]
