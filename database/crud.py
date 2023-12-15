import sqlite3

class Crud:

    """
        This class contains functions for CRUD operations.
        params: 
            db: str -- name of database.
    """

    def __init__(self, db):

        """
            Initialize connections with db and create a cursor object.
            params: 
                db: str -- name of database.
        """

        self.connection = sqlite3.connect(db)
        self.cursor = self.connection.cursor()

    def execute(self, query):

        """
            This function executes the query on the db.
        """

        self.connection.row_factory = sqlite3.Row
        if query.startswith("select"):
            result = []
            rows = self.cursor.execute(query).fetchall()
            columns = [column[0] for column in self.cursor.description]
            print(self.cursor.description)
            for row in rows:
                result.append(dict(zip(columns, row)))
        else:
            result = self.cursor.execute(query)
        self.connection.commit()

        return result
        

    def create(self,table_name, values_datatype):

        """
            Create operation on db.
            params:
                table_name: str -- name of table
                values_datatypes: dict -- dictionary containing values and their datatypes.
        """

        query = f"Insert into {table_name} " + " ({columns}) values " + "({values});"
        columns = ""
        values = ""
        print(values_datatype)
        for key,value in values_datatype.items():
            columns += f"{key} ,"
            if not value:
                values += 'Null ,'
            elif 'str' in str(type(value)):
                values += f"'{value}' ,"
            else:
                values += f"{value} ,"
                
        if values:
            values = values[:-1]
        if columns:
            columns = columns[:-1]

        query = query.format(columns = columns, values = values)

        print(query)
        result = self.execute(query)
        
    def read(self, table_name, columns, conditions = []):

        """
            Read operation on db.
            params:
                table_name:str -- name of table
                coliumn: str -- columns to be returned
                conditions: list -- filter conditions
        """
        #print(columns)
        query = f"select {','.join(columns)} from {table_name} where 1=1 "
        where_clause = ""

        if conditions:
            for condition in conditions:
                if condition["column"]=="column_filters":
                    continue
                where_clause += f" and {condition['column']} = {condition['value']} "

        query += where_clause

        print(query)
        result = self.execute(query)
        print(result)

        return result
        
    def update(self, table_name, old_value, new_value):

        """
            Update operation on db.
            params:
                table_name: str -- name of table
                old_value: dict -- old record
                new_value: dict -- updated record
        """

        query = f"Update {table_name} set "  +  "{set_clause} {where_clause}"
        set_clause = [f" {value[0]} = '{value[1]}'" if isinstance(value[1], str) else f" {value[0]} = {value[1]}"
         for value in new_value if value[1] is not None and value[0]!="EmployeeID" ]
        set_clause = " and ".join(set_clause)

        where_clause = [f" {value[0]} = '{value[1]}'" if isinstance(value[1], str) else f" {value[0]} = {value[1]}"
         for value in old_value if value[1] is not None]
        where_clause = " and ".join(where_clause)
        if where_clause:
            where_clause = " where " + where_clause

        query = query.format(set_clause=set_clause, where_clause=where_clause)

        print(query)
        
        result = self.execute(query)



    def delete(self, table_name, conditions):

        """
            Delete operation on db.
            params:
                table_name:str -- name of table
                conditions: list -- matching columns to filter records.
        """

        query = f"delete from {table_name} where 1=1"
        where_clause = ""

        for condition in conditions:
            where_clause += f" and {condition['column']} = {condition['value']} "

        query += where_clause
        print(query)
        result = self.execute(query)

        return {"Message": "Record deleted!!"}
        
        

if __name__=="__main__":

    test = Crud('akshit_db.db') 

    test.read('Employee', 'EmployeeID', 1)

    test.delete('Employee', 'EmployeeID', 1)
    test.read(1)


    values = {
        1: 'int',
        'Beth': 'str',
        'Nichols': 'str',
        'F': 'str',
        '2002-08-16': 'str', 
        'bethnichols@gmail.com': 'str', 
        'Sports development officer': 'str'
    }

    values2 = {
        "table_name" : "Employee",
        "values_datatype": {
                            "202": "int",
                            "Beth": "str",
                            "Nichols": "str",
                            "F": "str",
                            "2002-08-16": "str", 
                            "bethnichols@gmail.com": "str", 
                            "Sports development officer": "str"
    }
    }
    test.create('Employee', values)
    test.read('Employee', 'EmployeeID', 1)

    test.update('Employee', 'Gender', 'M', 'str', 'EmployeeId', 1)
    test.read('Employee', 'EmployeeID', 1)
    # where to include --self.connection.close()