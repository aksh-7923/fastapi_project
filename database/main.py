import random
import sqlite3

from faker import Faker 
fake = Faker()

# Create(if doesn't exist) and connect to the database
connection = sqlite3.connect('akshit_db.db')

# Create a cursor object
cursor = connection.cursor()

# drop table if already created
cursor.execute('''DROP TABLE IF EXISTS Employee''')

# Create a table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Employee (
        EmployeeID INTEGER PRIMARY KEY,
        FirstName TEXT,
        LastName TEXT,
        Gender TEXT,
        DateOfBirth TEXT,
        Email TEXT,
        Position TEXT
    )
''')

#insert data

def insert_record(number_of_records=200):

    """
        This function generates random record and inserts it into the database
    """

    for i in range(1, number_of_records+1):
        employee_id = i
        first_name = fake.first_name()
        last_name = fake.last_name()
        gender = random.choice(['M', 'F'])
        dob = fake.date_of_birth().strftime('%Y-%m-%d')
        email = first_name.lower() + last_name.lower() + '@gmail.com'
        position = fake.job()

        cursor.execute('''INSERT INTO Employee VALUES (?, ?, ?, ?, ?, ?, ?)''', 
        (employee_id, first_name, last_name, gender, dob, email, position))

insert_record()

# Commit the changes
connection.commit()

# select the data
cursor.execute('''select * from Employee limit 5''')
results = cursor.fetchall()   # Fetch the results
for row in results:           # Print or process the results as needed
    print(row)

# Close the connection
connection.close()


