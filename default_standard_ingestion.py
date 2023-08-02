import csv
import json
import time

from kafka import KafkaProducer


def read_csv(file_path):
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        headers = next(csvreader)
        for row in csvreader:
            yield row

def create_json(data_row):
    json_data = {
        "DataUnit": {
            "ifc.Person.ageGroup": data_row[0],  # Replace first column with "A"
            "ifc.Person.birthDate": data_row[1],  # Replace second column with "B"
            "ifc.Person.birthPlace": data_row[2],  # Replace third column with "C"
        }
    }
    # Add additional JSON fields if needed
    # json_data["AdditionalField"] = data_row[3]
    return json_data

def publish_to_kafka(bootstrap_servers, topic_name, data):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    for json_data in data:
        # Send the JSON data to Kafka
        producer.send(topic_name, value=json.dumps(json_data).encode('utf-8'))
        time.sleep(0.1)  # Add a small delay to control the speed of publishing

    producer.flush()
    producer.close()

if __name__ == "__main__":
    # Set your Kafka broker(s) here
    bootstrap_servers = 'localhost:9092'

    # Set the CSV file path and the Kafka topic name
    csv_file_path = '/path/to/your/csv/file.csv'
    kafka_topic_name = 'your_kafka_topic_name'

    # Read the CSV file and create JSON for each row
    data = []
    for row in read_csv(csv_file_path):
        json_data = create_json(row)
        data.append(json_data)

    # Publish the JSON data to Kafka
    publish_to_kafka(bootstrap_servers, kafka_topic_name, data)