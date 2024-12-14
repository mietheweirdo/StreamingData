from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

def print_data(data):
    print(f"Received from Kafka: {data}")

def main():
    # Tạo môi trường Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///D:/StudyMaterials/YEAR4/Cloud/StreamingData/connectors/flink-connector-kafka-3.4.0-1.20.jar")

    # Cấu hình Kafka Consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='sales_transactions',  # Kafka topic bạn đã tạo
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'localhost:9092',  # Kafka broker
            'group.id': 'flink-test-group',         # Group ID cho consumer
            'auto.offset.reset': 'earliest'
        }
    )

    # Đọc và xử lý dữ liệu
    stream = env.add_source(kafka_consumer)
    stream.map(print_data)

    # Chạy Flink job
    env.execute("Test Flink Kafka Connection")

if __name__ == "__main__":
    main()