import logging
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from yamlet.database import SessionLocal, engine
from yamlet.models.connections_model import Base, Connection
from yamlet.models.connections_schema import ConnectionCreate, ConnectionOut

# Create the connections table if it doesn't exist.
Base.metadata.create_all(bind=engine)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

router = APIRouter()

def get_db():
    """
    Dependency that provides a database session.

    Yields:
        Session: A SQLAlchemy session instance.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/connections", response_model=ConnectionOut)
def create_connection(conn: ConnectionCreate, db: Session = Depends(get_db)):
    """
    Create a new database connection record.

    Args:
        conn (ConnectionCreate): The connection details to create.
        db (Session, optional): The database session provided by dependency injection.

    Returns:
        ConnectionOut: The newly created connection record.
        
    Raises:
        HTTPException: If a connection with the same name already exists.
    """
    # Check if connection with the same name exists.
    existing = db.query(Connection).filter(Connection.name == conn.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Connection with this name already exists.")
    
    new_conn = Connection(**conn.dict())
    db.add(new_conn)
    db.commit()
    db.refresh(new_conn)
    return new_conn

@router.get("/connections", response_model=list[ConnectionOut])
def get_connections(db: Session = Depends(get_db)):
    """
    Retrieve all stored connection records.

    Args:
        db (Session, optional): The database session provided by dependency injection.

    Returns:
        list[ConnectionOut]: List of all connection records.
    """
    connections = db.query(Connection).all()
    return connections

@router.put("/connections/{conn_id}", response_model=ConnectionOut)
def update_connection(conn_id: int, conn: ConnectionCreate, db: Session = Depends(get_db)):
    """
    Update an existing connection record.

    Args:
        conn_id (int): The unique ID of the connection record to update.
        conn (ConnectionCreate): The new connection details.
        db (Session, optional): The database session provided by dependency injection.

    Returns:
        ConnectionOut: The updated connection record.

    Raises:
        HTTPException: If no connection is found with the provided ID.
    """
    connection_record = db.query(Connection).filter(Connection.id == conn_id).first()
    if not connection_record:
        raise HTTPException(status_code=404, detail="Connection not found.")
    for key, value in conn.dict().items():
        setattr(connection_record, key, value)
    db.commit()
    db.refresh(connection_record)
    return connection_record

@router.delete("/connections/{conn_id}")
def delete_connection(conn_id: int, db: Session = Depends(get_db)):
    """
    Delete a connection record.

    Args:
        conn_id (int): The unique ID of the connection record to delete.
        db (Session, optional): The database session provided by dependency injection.

    Returns:
        dict: A message confirming deletion.

    Raises:
        HTTPException: If no connection is found with the provided ID.
    """
    connection_record = db.query(Connection).filter(Connection.id == conn_id).first()
    if not connection_record:
        raise HTTPException(status_code=404, detail="Connection not found.")
    db.delete(connection_record)
    db.commit()
    return {"detail": "Connection deleted successfully."}
