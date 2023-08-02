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
		"ifc.Person.ageGroup": data_row[0],
		"ifc.Person.birthDate": data_row[1],
		"ifc.Person.birthPlace": data_row[2],
		"ifc.Person.civilName": data_row[3],
		"ifc.Person.cpf": data_row[4],
		"ifc.Person.deathDate": data_row[5],
		"ifc.Person.deputyId": data_row[6],
		"ifc.Person.electoralCondition": data_row[7],
		"ifc.Person.email": data_row[8],
		"ifc.Person.finalLegislatureCode": data_row[9],
		"ifc.Person.finalLegislatureElectionYear": data_row[10],
		"ifc.Person.finalLegislatureEnd": data_row[11],
		"ifc.Person.finalLegislatureStart": data_row[12],
		"ifc.Person.gender": data_row[13],
		"ifc.Person.initialLegislatureCode": data_row[14],
		"ifc.Person.initialLegislatureElectionYear": data_row[15],
		"ifc.Person.initialLegislatureEnd": data_row[16],
		"ifc.Person.initialLegislatureStart": data_row[17],
		"ifc.Person.lastStatusDate": data_row[18],
		"ifc.Person.maskID": data_row[19],
		"ifc.Person.name": data_row[20],
		"ifc.Person.party": data_row[21],
		"ifc.Person.situation": data_row[22],
		"ifc.Person.socialNetwork": data_row[23],
		"ifc.Person.state": data_row[24],
		"ifc_core_type":"DataUnit",
		"ifc_core_permission_dataSourceID":"Chamber_of_Deputies",
		"ifc_core_parent_type":"Person",
		"ifc_core_objectmodel":"entity",
		"ifc_core_active": true,
		"ifc_core_permission_classification": 1,
		"ifc_core_cms_creatingUserID":"External",
		"ifc_core_cms_updatingUserID":"External",
		"ifc_core_permission_investigationID":"any",
		"ifc_core_category":"Entities",
		"ifc_core_id_original": data_row[6]+"-Chamber_of_Deputies-front",
		"ifc_core_parentID_original":"Person-"+data_row[6],
	    },
	    "identifiers": [
		    {
			    "identifierType":"deputy ID",
			    "value": data_row[6],
			    "isUnique": true
		    }
	    ]
    }


    return json_data

def publish_to_kafka(bootstrap_servers, topic_name, data):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    for json_data in data:
        producer.send(topic_name, value=json.dumps(json_data).encode('utf-8'))
        time.sleep(0.1)  # Add a small delay to control the speed of publishing

    producer.flush()
    producer.close()

if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    csv_file_path = './deputados.csv'
    kafka_topic_name = 'ingestion_consumer_topic'

    data = []
    for row in read_csv(csv_file_path):
        json_data = create_json(row)
        data.append(json_data)

    publish_to_kafka(bootstrap_servers, kafka_topic_name, data)