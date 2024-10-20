import os
from dotenv import load_dotenv

load_dotenv()

# Environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MONGO_DB_URI = os.getenv("MONGO_DB_URI")