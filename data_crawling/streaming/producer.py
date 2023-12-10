import os
import sys 
sys.path.insert(
    0, os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ), 
    "..", "..")
)
from kafka import KafkaProducer

from datetime import datetime
import json
import time

from data_crawling.config import DIR_LISTENER

from data_crawling.utils.directory_listener import file_listener
from data_crawling.utils.file_manipulator import extract_prefixes


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"]
)

count_null = 0
while True:
    with open(DIR_LISTENER, 'r', encoding='utf-8') as nf:
        new_files = nf.readlines()

    if len(new_files) == 0:
        count_null += 1
        producer.send(
            topic = "tweets-streaming",
            value = f'null_{count_null}'.encode("ascii"),
            key = 'null_message'.encode('ascii'),
        )
        time.sleep(5)

    else:
        for file_dir in new_files:
            file_dir = file_dir.replace('\n', '')
            key = extract_prefixes(file_dir)
            with open(file_dir, 'r', encoding='utf-8') as file:
                data = json.load(file)

            num_partition = 5 
            for i in range(0, len(data), num_partition):
                for j in range(num_partition):
                    if i + j >= len(data):
                        break

                    value = json.dumps(data[i + j])
                    producer.send(
                        topic = "tweets-streaming",
                        value = value.encode("ascii"),
                        partition = j,
                        key = f'{key}'.encode('ascii'),
                        timestamp_ms = int(datetime.now().timestamp())
                    )
            
            with open(DIR_LISTENER, 'r', encoding='utf-8') as fin:
                data = fin.read().splitlines(True)
            with open(DIR_LISTENER, 'w', encoding='utf-8') as fout:
                fout.writelines(data[1:])





