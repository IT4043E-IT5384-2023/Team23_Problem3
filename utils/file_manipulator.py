import os
import sys 
sys.path.insert(
    0, os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)
        ), 
    "..")
)
import re
import json
import datetime
from datetime import date, datetime


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def save_to_file(data, filename):
    # If the file exists, load the existing data
    if os.path.exists(filename):
        with open(filename, "r") as f:
            file_data = json.load(f)

        file_data.extend(data)

        # Save the updated data
        with open(filename, "w") as f:
            json.dump(file_data, f, indent=4, default=json_serial)

    else:
        with open(filename, "w") as f:
            json.dump(data, f, indent=4, default=json_serial)


def get_keywords_from_json(filename):
    with open(filename, "r", encoding='utf-8') as f:
        data = json.load(f)

    keywords = []
    for keyword in data['data']:
        keywords.append(keyword["name"])

    return keywords


def get_user_id_from_json(file_name):
    with open(file_name, "r", encoding='utf-8') as f:
        data = json.load(f)

    user_id = []
    for tweet in data:
        user_id.append(tweet['user']["id"])
    user_id = list(set(user_id))
    return user_id
        

def extract_prefixes(file_paths):
    arr = file_paths.split('/')
    topic_name = arr[-1].replace('.json', '')
    return topic_name


if __name__ == "__main__":
    # List of file paths
    file_paths = [
        "/home/quangbinh/big_data_storage/Team23_Problem3/data/raw/test/PMXX_tweets.json",
        "/home/quangbinh/big_data_storage/Team23_Problem3/data/raw/test/GameZone_tweets.json",
        # Add more file paths as needed
    ]

    # Call the function to extract prefixes
    prefixes = extract_prefixes(file_paths)

    # Print the extracted prefixes
    for prefix in prefixes:
        print(prefix)

