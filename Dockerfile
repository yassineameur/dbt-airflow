
# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.8-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True
ENV PORT 8000

ENV APP_HOME /app
ENV DBT_PROFILES_DIR $APP_HOME


# Copy local code to the container image.
WORKDIR $APP_HOME
COPY requirements.txt .

RUN apt-get update
RUN apt-get -y install gcc
RUN apt-get -y install git

# Install production dependencies.
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get remove -y gcc git &&  apt-get autoremove -y

COPY ./api_dbt ./

CMD gunicorn main:app -c api/gunicorn_config.py
