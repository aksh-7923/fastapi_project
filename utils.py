from faker import Faker
import random

fake = Faker()

for i in range(1, 201):
    employee_id = i
    first_name = fake.first_name()
    last_name = fake.last_name()
    gender = random.choice(['M', 'F'])
    dob = fake.date_of_birth().strftime('%Y-%m-%d')
    email = first_name.lower() + last_name.lower() + '@gmail.com'
    position = fake.job()

    print(f"({employee_id}, '{first_name}', '{last_name}', '{gender}', '{dob}', '{email}', '{position}'),")