FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

# apt update
RUN apt-get update && \
    apt -y upgrade && \
    apt-get install -y software-properties-common && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update

# install python
RUN apt-get install -y python3.11 python3.11-dev python3.11-distutils build-essential

# install mysql, postgres clients
RUN apt-get install -y libmysqlclient-dev mysql-client

# install utilities
RUN apt-get install -y curl

# Set working directory
WORKDIR /opt/sbosc

# Make python 3.11 the default
# Register the version in alternatives
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1

# Set python 3 as the default python
RUN update-alternatives --set python /usr/bin/python3.11

# Install pip and requirements.txt
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python

# Install requirements
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy repository
COPY src ./
ENV PYTHONPATH=/opt/sbosc
