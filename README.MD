# Dashboard OpenWeatherMap

A containerized data pipeline and visualization dashboard that integrates OpenWeatherMap data. This project uses **Docker**, **Airflow**, **PostgreSQL**, and **Grafana** for real-time weather data retrieval, storage, and visualization.

---

## Features

- **Airflow** for orchestrating data pipelines.
- **PostgreSQL** as the database backend for storing weather data.
- **Grafana** for creating and managing real-time dashboards.
- Fully containerized setup using **Docker Compose** for easy deployment and replication.

---

## Prerequisites

Ensure the following tools are installed on your system:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

---

## Installation

### 1. Clone the repository
Clone the project repository to your local system:  
```bash
git clone https://github.com/rhaikal/dashboard-openweathermap
cd dashboard-openweathermap
```

### 2. Create Network
Create a Docker network for the services:  
```bash
docker network create open-weather-map
```

### 3. Start the Environment
Use Docker Compose to build and start all services:
```bash
docker compose up
```

### 4. Adjust Permissions (Optional)
If you encounter permission issues, run the following command to ensure proper file ownership:
```bash
sudo chown -R "${USER:-$(id -un)}" .
```

## Services & Endpoints

### Grafana Dashboard
- Port: `3001`
- Username: `admin`
- Password: `admin`

### Airflow
- Port: `8080`
- Username: `airflow`
- Password: `airflow`

### PostgreSQL Database
- Port: `5435`
- Username: `airflow`
- Password: `airflow`