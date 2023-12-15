from typing import List
from .models import Employee
from database.crud import Crud
from fastapi import FastAPI, HTTPException, status, Request

# creating fastapi instance
app = FastAPI()

# Create (POST) endpoint
@app.post("/emp/post")
def create_item(record: Employee):
    
    """
        execute create function from crud.py
    """
    
    TABLE_NAME = "Employee"
    Crud("akshit_db.db").create(table_name=TABLE_NAME, values_datatype=record.dict())

    return {"Message": "record created !!"}


# Read (GET) endpoint
@app.get("/emp/read")
def read_item(request: Request, column_filters: str = "*" ) ->list[Employee]:

    """
        execute read function from crud.py
    """
    
    TABLE_NAME = "Employee"
    query_params = dict(request.query_params)
    columns = query_params.get('column_filters', column_filters)
    columns = columns.split(",")
    conditions = []
    for key, value in query_params.items():
        condition = {"column": key, "value": value}
        conditions.append(condition)
    
    results = Crud("akshit_db.db").read(table_name = TABLE_NAME, columns= columns, conditions=conditions)
    
    response = []
    for result in results:
        result = Employee(**result)
        response.append(result)

    return response


# Update (PUT) endpoint
@app.put("/emp/update")
def update_item(old_employee: Employee, new_employee: Employee):

    """
        execute update function from crud.py
    """

    TABLE_NAME = "Employee"

    Crud("akshit_db.db").update(table_name = TABLE_NAME, old_value=old_employee, new_value=new_employee)

    return {"Message": "record updated !!"}
    

# Delete (DELETE) endpoint
@app.delete("/emp/delete")
def delete_item(request: Request):

    """
        execute delete function from crud.py
    """
    TABLE_NAME = "Employee"
    query_params = dict(request.query_params)
    conditions = []
    for key, value in query_params.items():
        condition = {"column": key, "value": value}
        conditions.append(condition)
    Crud("akshit_db.db").delete(table_name = TABLE_NAME, conditions=conditions)
    return {"Message": "record deleted !!"}
