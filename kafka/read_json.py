import json

def read_json(file_path):
    output = []
    # Read the JSON file
    with open("output.json", "r") as file:
        # Load the content of the file as a JSON array of strings
        json_strings = json.load(file)
        print(json_strings)


    return json_strings

if __name__=="__main__":
    read_json("output.json")

    # Process each JSON string in the array
    # for json_string in json_strings:
    #     # Parse the JSON string to get a Python dictionary
    #     # Now 'data' is a dictionary representing each individual JSON object
    #     data = json.loads(json.dumps(json_string))   
    #     print(data)
    #     output.append(data)
    
