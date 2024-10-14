from fastapi import APIRouter
from controllers.news_controller import NewsController
from validators.news_validator import NewsQueryModel
from typing import Dict

router = APIRouter()

controller = NewsController()

@router.post("/count")
async def count_news(query: NewsQueryModel) -> Dict[str, int]:
    """
    API route that receives a JSON payload with key-value pairs 
    and returns the count of rows in MongoDB that match the query.

    Args:
        query (NewsQueryModel): The query parameters in the request body.

    Returns:
        dict: A dictionary containing the count of matching rows.
    """
    return controller.count_matching_news(query)