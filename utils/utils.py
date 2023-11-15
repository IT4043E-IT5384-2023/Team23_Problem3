import os
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


