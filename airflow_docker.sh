#!/bin/bash

echo "======================================"
echo "   Initializing Airflow Database      "
echo "======================================"

if docker compose up airflow-init; then
    echo "Airflow database initialized successfully."
else
    echo "Error: Failed to initialize the Airflow database."
    exit 1
fi

echo
echo "======================================"
echo " Starting Docker Containers Detached  "
echo "======================================"

if docker compose up -d; then
    echo "Docker containers started successfully."
else
    echo "Error: Failed to start Docker containers."
    exit 1
fi

echo
echo "======================================"
echo "    All Services Started Successfully "
echo "======================================"

echo "Listing All running containers"

docker ps
