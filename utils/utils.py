import re

def format_string(input_string):
    output_string = re.sub(r'\n', ' ', input_string)
    return output_string


def find_user_tag_from_string(input_string):
    matches = re.findall(r'@(\S+)', input_string)
    matches = ['@' + match for match in matches]
    return matches



if __name__ == "__main__":

    input_string = "This is a line.\nThis is another line.\nAnd one more line."

    result = format_string(input_string)
    print(result)

        
    input_string = 'GEMS @LADY #BSCGEM @JacksonGems9dakjlsdh sada10h'

    mentions = find_user_tag_from_string(input_string)
    print(mentions)
