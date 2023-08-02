import csv
import json
import time
from kafka import KafkaProducer

def read_csv(file_path):
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        headers = next(csvreader)
        for row in csvreader:
            yield headers, row

def normalize_to_json(headers, data_row):
    # Create a JSON template using headers and data_row
    json_template = {}
    for header, value in zip(headers, data_row):
        json_template[header] = value
    return json.dumps(json_template)

def stream_csv_to_kafka(file_path, bootstrap_servers, topic_name):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    for headers, row in read_csv(file_path):
        json_data = normalize_to_json(headers, row)
        # Send the normalized JSON data to Kafka
        producer.send(topic_name, value=json_data.encode('utf-8'))
        time.sleep(0.1)  # Add a small delay to control the speed of streaming

    producer.flush()
    producer.close()

if __name__ == "__main__":
    # Set your Kafka broker(s) here
    bootstrap_servers = 'localhost:9092'

    # Set the CSV file path and the Kafka topic name
    csv_file_path = '/path/to/your/csv/file.csv'
    kafka_topic_name = 'your_kafka_topic_name'

    stream_csv_to_kafka(csv_file_path, bootstrap_servers, kafka_topic_name)

