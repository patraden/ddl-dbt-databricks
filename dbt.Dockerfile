FROM python:3.8.1-slim-buster

LABEL maintainer="d.patrakhin@ddl.com"
ARG PROFILES_FILE
# Set working directory

WORKDIR /app

# Install OS dependencies
RUN apt-get update && apt-get install -qq -y \
    git gcc build-essential libpq-dev --fix-missing --no-install-recommends \
    && apt-get clean

# Make sure we are using latest pip
RUN pip install --upgrade pip

# Create directory for dbt config
RUN mkdir -p /root/.dbt

# Copy requirements.txt
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip install -r requirements.txt

# Copy dbt profile
RUN echo "${PROFILES_FILE}" > /root/.dbt/profiles.yml

# Copy databricks project
COPY ddl-databricks /app

# Run debug to validate the connectivity

EXPOSE 8080

WORKDIR /app/ddl-databricks
CMD ["/bin/bash", "-c", "dbt debug; dbt docs generate; dbt docs serve"]
