from pyflink.table.expressions import col, lit
from pyflink.table.types import DataTypes
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.window import Tumble
from pyflink.table import EnvironmentSettings, TableEnvironment
import logging

# PostgreSQL Configuration
POSTGRES_URL = "jdbc:postgresql://postgres:5432/retail_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "123"

KAFKA_TOPIC = "sales_transactions"
KAFKA_BROKER = "broker:29092"

def setup_environment():
    """Sets up the Flink execution and table environments."""
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(env_settings)
    return table_env

def configure_kafka_source(table_env):
    """Configures the Kafka source using SQL DDL."""
    table_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            transaction_id BIGINT,
            product_id INT,
            quantity INT,
            price FLOAT,
            `timestamp` TIMESTAMP(3),
            customer_id INT,
            store_id INT,
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = 'testGroup',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'scan.startup.mode' = 'latest-offset',
            'json.fail-on-missing-field' = 'true',
            'json.ignore-parse-errors' = 'false'
        )
    """)

def calculate_metrics_with_windows(table_env):
    """Calculates metrics using tumbling windows."""
    transactions = table_env.from_path("kafka_source")

    # Define a tumbling window (10-second window)
    windowed = transactions.window(
        Tumble.over(lit(10).seconds).on(col("timestamp")).alias("w")
    ).group_by(col("w"), col("product_id"), col("store_id"))

    # 1. Tổng doanh thu (tổng quantity * price)
    total_revenue = transactions.select(
        (col("quantity") * col("price")).sum.alias("total_revenue"),
        lit(1).alias("dummy_id")  # Add dummy_id for primary key
    )

    # 2. Tổng số lượng sản phẩm bán ra
    total_quantity = transactions.select(
        col("quantity").sum.alias("total_quantity"),
        lit(1).alias("dummy_id")  # Add dummy_id for primary key
    )

    # 3. Doanh thu theo từng sản phẩm
    revenue_per_product = windowed.select(
        col("product_id"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        (col("quantity") * col("price")).sum.alias("product_revenue")
    )

    # 4. Doanh thu theo từng cửa hàng
    revenue_per_store = windowed.select(
        col("store_id"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        (col("quantity") * col("price")).sum.alias("store_revenue")
    )

    return total_revenue, total_quantity, revenue_per_product, revenue_per_store

def configure_postgresql_sink(table_env: TableEnvironment, table_name: str, schema: dict, primary_key: str):
    """Configures the PostgreSQL sink using SQL DDL."""
    schema_fields = ", ".join([f"{field} {dtype}" for field, dtype in schema.items()])
    
    try:
        # Drop table if exists
        logging.info(f"Dropping table if exists: {table_name}")
        table_env.execute_sql(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create table
        logging.info(f"Creating table: {table_name}")
        table_env.execute_sql(f"""
            CREATE TABLE {table_name} (
                {schema_fields},
                PRIMARY KEY ({primary_key}) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{POSTGRES_URL}',
                'table-name' = '{table_name}',
                'username' = '{POSTGRES_USER}',
                'password' = '{POSTGRES_PASSWORD}'
            )
        """)
        logging.info(f"Table {table_name} created successfully.")
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")

def query_kafka_data(table_env):
    """Queries data from the Kafka source."""
    transactions = table_env.from_path("kafka_source")
    result = transactions.select(col("*"))
    transactions.execute_insert("print_sink")

def main():
    """Main function to set up and execute the Flink job."""
    # Set up environments
    table_env = setup_environment()

    # Configure Kafka source
    configure_kafka_source(table_env)

    # Create print sink
    table_env.execute_sql("""
        CREATE TEMPORARY TABLE print_sink (
            transaction_id BIGINT,
            product_id INT,
            quantity INT,
            price FLOAT,
            `timestamp` TIMESTAMP(3),
            customer_id INT,
            store_id INT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Query Kafka data
    query_kafka_data(table_env)

    # Calculate metrics
    total_revenue, total_quantity, revenue_per_product, revenue_per_store = calculate_metrics_with_windows(table_env)

    # Configure PostgreSQL sinks
    configure_postgresql_sink(table_env, "total_revenue", {"total_revenue": "FLOAT", "dummy_id": "INT"}, "dummy_id")
    configure_postgresql_sink(table_env, "total_quantity", {"total_quantity": "BIGINT", "dummy_id": "INT"}, "dummy_id")
    configure_postgresql_sink(
        table_env,
        "product_revenue",
        {
            "product_id": "INT",
            "window_start": "TIMESTAMP",
            "window_end": "TIMESTAMP",
            "product_revenue": "FLOAT",
        },
        "product_id, window_start",
    )
    configure_postgresql_sink(
        table_env,
        "store_revenue",
        {
            "store_id": "INT",
            "window_start": "TIMESTAMP",
            "window_end": "TIMESTAMP",
            "store_revenue": "FLOAT",
        },
        "store_id, window_start",
    )

    # Write metrics to PostgreSQL without waiting
    total_revenue.execute_insert("total_revenue")
    total_quantity.execute_insert("total_quantity")
    revenue_per_product.execute_insert("product_revenue")
    revenue_per_store.execute_insert("store_revenue")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()