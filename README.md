# Streaming Data with Kafka, Flink, and PostgreSQL

## Programming Language: Python

## Project Description
**Exercise:** Building an Online Data System for a Retail Store with Kafka, Flink, and Postgres

### Objectives
Students will build an online data processing system for a retail store. This system will collect real-time sales transaction data, process it, and store it for analysis and reporting purposes.

### Scenario
A retail store chain wants to build an online data system to collect and analyze sales transaction data. Each transaction includes information such as product code, quantity, price, transaction time, and customer information.

### Task Description
1. **Environment Setup:**
   - Install Kafka, Flink, and Postgres using Docker.

2. **Kafka Producer:**
   - Create a producer to send sales transaction data to a Kafka topic.

3. **Kafka Consumer:**
   - Use Flink to read data from Kafka.

4. **Data Processing with Flink:**
   - Calculate metrics such as total revenue and the number of products sold in real-time.

5. **Store Results:**
   - Store the processed results in a Postgres database.

6. **Query and Verify:**
   - Query Postgres to verify and report the processed results.

### Detailed Steps
1. **Environment Setup:**
   - Install Docker.
   - Create Docker Compose file to set up Kafka, Flink, and Postgres.

2. **Kafka Producer:**
   - Write a Python program to send sales transaction data to a Kafka topic. Each transaction includes fields: `transaction_id`, `product_id`, `quantity`, `price`, `timestamp`, `customer_id`.
   - Ensure data is sent regularly and continuously.

3. **Kafka Consumer with Flink:**
   - Configure Flink to connect and read data from the Kafka topic.
   - Ensure Flink can consume data from the Kafka topic accurately.

4. **Data Processing with Flink:**
   - Perform data processing operations in Flink, including:
     - Calculate total revenue (total `quantity` * `price`).
     - Calculate the total number of products sold.
     - Calculate revenue per product.
     - Calculate revenue per store (if applicable).

5. **Store Results in Postgres:**
   - Connect Flink to the Postgres database.
   - Store the processed results in tables in Postgres, e.g., `total_revenue`, `product_sales`, `store_sales`.

6. **Query and Verify:**
   - Write SQL queries to verify the processed results in Postgres.
   - Ensure the data in Postgres accurately reflects the processed results from Flink.

### How to Run the Project

#### Prerequisites
- Docker and Docker Compose installed on your machine.
- Python installed on your machine.

#### Step-by-Step Guide

1. **Clone the Repository:**
 ```sh
   git clone <repository-url>
   cd <repository-directory>
 ```
2. **Start the Services:**
Run the following command to start Kafka, Flink, and Postgres using Docker Compose:
 ```sh
   docker-compose up -d
 ```
4. **Create venv**
3. **Install packages**
 ```sh
   pip install -r requirements.txt
 ```
5. **Run Kafka producer script**
```sh
    python kafka_producer.py
```
6. **Run Flink job**
 ```sh
   docker compose exec -it jobmanager bash
   flink run -py examples/flink-kafka-consumer.py
 ```
7. **Connect to the Postgres database**
```sh
    docker exec -it postgres bash
    psql -h localhost -U postgres -d retail_store
```
8. **Run SQL queries to verify the processed results**
```sh
    SELECT * FROM total_revenue;
    SELECT * FROM total_quantity;
    SELECT * FROM product_sales;
    SELECT * FROM store_sales;
```