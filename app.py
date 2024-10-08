from typing import Dict
from pymongo import MongoClient
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from services.dataloader import load_data
from services.mongo_operations import count_documents_in_mongo
from validate import QueryModel
from services.logger import get_logger

# Create a logger instance
logger = get_logger()

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions
    load_data(collection)
    yield
    # Add any shutdown actions here if needed

app = FastAPI(lifespan=lifespan)

@app.post("/count")
async def count_rows(query: QueryModel)-> Dict[str, int]:
    """
    API route that receives a JSON payload with key-value pairs 
    and returns the count of rows in MongoDB that match the query.

    Args:
        query (QueryModel): The query parameters in the request body.

    Returns:
        dict: A dictionary containing the count of matching rows.
    """
    try:
        count = count_documents_in_mongo(collection, query)
        return {"count": count}

    except Exception as e:
        logger.error(status_code=500, detail=f"Error querying database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error querying database: {str(e)}")

try:
    # MongoDB setup
    client = MongoClient("mongodb://localhost:27017/")
    db = client["task1"]
    collection = db["bbc_news"]
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.critical(f"Error connecting to MongoDB: {str(e)}")
    raise RuntimeError(f"Error connecting to MongoDB: {str(e)}")

