import csv
import json
import time
from kafka import KafkaProducer


bootstrap_servers = 'localhost:9092'
kafka_topic_name = 'ingestion_consumer_topic'
data = 'data_unit.json'

publish_to_kafka(bootstrap_servers, kafka_topic_name, data)



# https://aiven.io/developer/create-your-own-data-stream-for-kafka-with-python-and-faker


