import os
import sys 
sys.path.insert(
    0, os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ), 
    "..")
)

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'tweets-streaming',  
    bootstrap_servers=['localhost:9092'],  
    auto_offset_reset='latest', 
    enable_auto_commit=True,
)

output_dir = '/home/code/Team23_Problem3/data_crawling/data/raw/test_consumer'

message_list = []
previous_key = 'null_message'
for message in consumer:
    key = message.key.decode('ascii')
    value = message.value.decode('ascii')
    print(key)
    if previous_key != key and previous_key != 'null_message':
        filename = os.path.join(output_dir, f"{previous_key}.json")
        with open(filename, 'w', encoding='utf-8') as json_file:
            json.dump(message_list, json_file, indent=4)
        message_list = []

    previous_key = key
    if key != 'null_message':
        print(f'Message not null.')
        message_list.append(json.loads(value))

  
