FROM python:3.12
WORKDIR /usr/local/app
COPY . /usr/local/app
RUN pip install --no-cache-dir -r /app/requirements.txt
EXPOSE 5000

# Create a non-root user and switch to it
RUN useradd app
USER app

# This Dockerfile sets up a Python environment, copies the application code into the container,
