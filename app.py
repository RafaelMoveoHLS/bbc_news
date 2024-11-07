from pymongo import MongoClient
from fastapi import FastAPI
from contextlib import asynccontextmanager
from services.dataloader import load_data
from services.logger import get_logger
from routers.news_router import router as news_router

# Create a logger instance
logger = get_logger()

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup actions
    logger.info("App is starting up...")
    load_data()
    yield
    # Shutdown actions
    logger.info("App is shutting down...")

app = FastAPI(lifespan=lifespan)

app.include_router(news_router, prefix="/news", tags=["News"])
