def get_column_lists(index: int, data):
    output = [desc[index] for desc in data]
    output.pop(0)
    output.pop(-1)
    return output

def format_strings_list(input):
    output = ''
    for x in range(len(input)):
        output +=  input[x] + ', '

    output = output[:-2]
    return output

def normalize_value(value):
    if isinstance(value, (int, float, str)) and value is not None:
        try:
            return float(value)
        except ValueError:
            pass 
    return value