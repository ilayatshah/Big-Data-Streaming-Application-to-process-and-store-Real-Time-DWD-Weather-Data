# Python Weather Data Downloader

This repository contains a collection of six Python applications designed to download weather data for the entire Germany region. The data includes various parameters such as wind, extreme wind, precipitation, solar, temperature, and extreme temperature with a temporal resolution of 10 minutes. The applications send the collected data to a Kafka topic using either a native Kafka producer or a REST Kafka producer. Each application exposes Prometheus metrics at http://localhost:8000/metrics and logs its activities to a file named "(python filename).log" located in the working directory.

## Prerequisites

Docker Setup
Clone the repository from GitLab.

## Set the required environment variables for your system:

$env:HTTP_PROXY = "http://your.proxy.server:port"

$env:HTTPS_PROXY = "https://your.proxy.server:port"

$env:PASSWORD = "password"

## Set the environment variable for the producer method you want to use:
### For the native Kafka producer:
$env:PRODUCER_METHOD = "native_kafka_producer"

### For the REST Kafka producer:
$env:PRODUCER_METHOD = "rest_kafka_producer"

## Build the Docker image:
docker build -