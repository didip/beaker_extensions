#!/bin/bash

virtualenv ./venv && source ./venv/bin/activate
pip install -r requirements.txt

docker pull redis
docker run -d -p 6379:6379 redis

docker pull cassandra:3.0.17
docker run -d -p 9042:9042 cassandra:3.0.17

