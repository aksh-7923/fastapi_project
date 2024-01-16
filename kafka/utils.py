# utility class containing all the required functions
import json
import pandas as pd
from pathlib import Path


class Utility:

    def __init__(self):
        self.file_path = "output.json"


    def get_file(self, mode):
        if mode!="w" and not Path(self.file_path).exists():
            with open(self.file_path, "w") as file:
                file.write(json.dumps([]))

        file = open(self.file_path, mode)

        return file


    def read(self):
        file = self.get_file( "r")
        try:
            data = json.load(file)
            data = pd.DataFrame(data)

            return data
        except json.JSONDecodeError:
            # Handle the case when the file is empty or not a valid JSON
            return pd.DataFrame()  # Return an empty DataFrame

        finally:
            file.close()

    
    def write(self, data_df):
        data_df.to_json(self.file_path, orient = "records", indent = 4)


    def get_conditions(self, old_record):
        conditions = ""
        for key, value in old_record.items():
            if isinstance(value, str):
                condition = f"({key} == '{value}') & "
            else:
                condition = f"({key} == {value}) & "
            conditions += condition
        conditions = conditions[:-3]        
        print(conditions)

        return f"{conditions}"


    def filter_records(self, df, filters):
        condition = self.get_conditions(filters)
        indexes = df.query(condition).index.tolist()

        return indexes


    def read_record(self, filters):
        file_df = self.read()
        idx_to_read = self.filter_records(file_df, filters)
        selected_rows = file_df.loc[idx_to_read]
        print(selected_rows.to_json(orient='records', lines=True, indent=4))


    def add_record(self, record):
        file_df = self.read()
        # print(file_df)
        if record:
            record_df = pd.DataFrame([record])
        else:
            print("Empty record!!")
        
        file_df = pd.concat([file_df, record_df], ignore_index=True)
        self.write(file_df)


    def update_record(self, payload):
        
        old_employee = payload["old_employee"]
        new_employee = payload["new_employee"]
        print(new_employee)
        file_df = self.read()
        matching_idx = self.filter_records(file_df, old_employee)
        #print(matching_idx)
        # file_df.loc[matching_idx] = new_employee
        file_df.loc[matching_idx, list(new_employee.keys())] = list(new_employee.values())
        self.write(file_df)


    def delete_record(self, record):

        file_df = self.read()
        idxs_to_drop = self.filter_records(file_df, record)
        # Drop rows by index
        # print(idxs_to_drop)
        file_df.drop(index = idxs_to_drop, inplace=True)
        self.write(file_df)


if __name__=="__main__":
    record = {
        "FirstName":"Aman",
        "LastName":"Mishra",
        "Gender":"M",
        "DateOfBirth":"1993-04-28",
        "Email":"robertowright@gmail.com",
        "Position":"IT sales professional"
    }

    payload = { 
    "old_employee": {
        "FirstName":"Aksit"
    },
    "new_employee" : {
        "Gender":"M", 
        "FirstName": "Akshit"
    }
            }

    record1 = {
        "FirstName":"Nicole"
    }
    record2 = {"FirstName": "Don", "LastName": "French", "Gender": "F", "DateOfBirth": "1990-04-11", 
            "Email": "jonfrench@gmail.com", "Position": "Diplomatic Services operational officer"}

    # Utility().write_json("output.json", record1)
    # Utility().add_record("output.json", record)
    # Utility().update_record("output.json", payload)
    # Utility().get_conditions(record1)
    # Utility().delete_record("output.json", record1)
    Utility().read_record(record1)
