import os
from dotenv import load_dotenv

load_dotenv()

# Environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MONGO_DB_LOCAL_URI = os.getenv("MONGO_DB_URI")
MONGO_DB_ATLAS_URI = os.getenv("MONGO_DB_URI_ATLAS")