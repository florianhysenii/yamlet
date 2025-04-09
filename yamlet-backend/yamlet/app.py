# app.py
from fastapi import FastAPI
from yamlet.endpoints import pipeline
from yamlet.endpoints import connections

app = FastAPI(
    title="My ETL API",
    description="API to run ETL pipelines and manage connection configurations.",
    version="1.0.0"
)

app.include_router(pipeline.router)
app.include_router(connections.router)
