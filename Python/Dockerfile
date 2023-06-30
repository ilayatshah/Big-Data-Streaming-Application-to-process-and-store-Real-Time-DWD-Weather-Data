# Use the official Python base image
FROM python:3

# Install dependencies
RUN pip install prometheus_client kafka-python pandas

# Set the working directory
WORKDIR /app

# Copy the contents of the current directory to the container's working directory
COPY . /app

# Set the entrypoint to run Python
ENTRYPOINT [ "python" ]






