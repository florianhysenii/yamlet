from pydantic import BaseModel

class ConnectionBase(BaseModel):
    """
    Base schema for connection details.
    
    Attributes:
        name (str): Unique connection name.
        type (str): Database type.
        host (str): Database host address.
        port (int): Port number.
        database (str): Database name.
        user (str): Username.
        password (str): Password.
    """
    name: str
    type: str
    host: str
    port: int
    database: str
    user: str
    password: str

class ConnectionCreate(ConnectionBase):
    """
    Schema for creating a new connection.
    """
    pass

class ConnectionOut(ConnectionBase):
    """
    Schema representing a connection output, including its unique ID.
    """
    id: int

    class Config:
        orm_mode = True
