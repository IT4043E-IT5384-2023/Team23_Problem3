import os
import re
import json

def format_string(input_string):
    output_string = re.sub(r'\n', ' ', input_string)
    return output_string


def find_user_tag_from_string(input_string):
    matches = re.findall(r'@(\S+)', input_string)
    matches = ['@' + match for match in matches]
    return matches


def write_to_file(data: dict=None, name: str=None) -> None:
    """
    Function to write json data to file
    If file does not exist, create a new one and add data to it
    If file already have data, do nothing.
    """
    if not os.path.isfile(name):
        with open(name, 'a') as file:
            file.write(json.dumps(data))
    else:
        print(f"File {name} already exists.")


if __name__ == "__main__":

    input_string = "This is a line.\nThis is another line.\nAnd one more line."

    result = format_string(input_string)
    print(result)

        
    input_string = 'GEMS @LADY #BSCGEM @JacksonGems9dakjlsdh sada10h'

    mentions = find_user_tag_from_string(input_string)
    print(mentions)
