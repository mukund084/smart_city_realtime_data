# Smart City Real-Time Data Pipeline

This repository contains code for a real-time data pipeline designed for a smart city infrastructure, capable of simulating and processing data from multiple data streams such as vehicle movements, GPS tracking, traffic camera feeds, weather conditions, and emergency incidents. The project showcases an end-to-end data engineering setup leveraging Apache Kafka for data ingestion, Apache Spark for data processing, and Amazon S3 for data storage, all orchestrated using Docker.

## Project Overview

The system simulates real-time data generation of vehicle and environmental conditions in a smart city context. This includes:

- **Vehicle data:** Simulates car movements with attributes like speed, direction, and location.
- **GPS data:** Tracks real-time coordinates and movement.
- **Traffic camera data:** Captures snapshots from various city locations.
- **Weather data:** Monitors weather conditions such as temperature, humidity, and air quality.
- **Emergency data:** Records emergency incidents with relevant details.

All components are containerized using Docker, ensuring easy scalability and deployment across different environments.

## Architecture

1. **Kafka**: Used for real-time message queuing.
2. **Spark**: Processes the streamed data and performs transformations.
3. **S3**: Stores the processed data for persistence and further analysis.

### Components

- **Zookeeper & Kafka Brokers**: Manage and maintain messaging across producers and consumers.
- **Spark Master & Workers**: Facilitate distributed data processing.
- **Custom Python Scripts**: Simulate real-time data generation for various urban scenarios.

## Setup and Deployment

The project uses `docker-compose` for easy setup and deployment. Each component of the system, including Kafka brokers, Zookeeper, and Spark nodes, is defined in the `docker-compose.yml` file.

### Requirements

- Docker
- Docker Compose

### Launching the Project

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/smart_city_realtime_data.git


### Tutorial Reference

This project follows the tutorial from the YouTube video [Smart City End to End Realtime Data Engineering Project | Get Hired as an AWS Data Engineer](https://www.youtube.com/watch?v=Vv_fvwF41_0) by CodeWithYu . The tutorial provided a foundational framework and guidance for building this real-time data pipeline.
