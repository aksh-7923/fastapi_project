from typing import Optional
from pydantic import BaseModel

class Employee(BaseModel):
    FirstName: Optional[str] = None
    LastName: Optional[str] = None 
    Gender: Optional[str] = None 
    DateOfBirth: Optional[str] = None 
    Email: Optional[str] = None
    Position: Optional[str] = None 