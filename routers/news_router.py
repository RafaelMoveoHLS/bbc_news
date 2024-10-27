from fastapi import APIRouter, Query
from controllers.news_controller import NewsController
from validators.news_validator import NewsQueryModel
from typing import Dict, List, Any

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

@router.get("/search")
async def semantic_news_search(query: str = Query(..., description="The search query")) -> Dict[str, Any]:
    """
    API route that receives a search query as a query parameter and returns related news articles
    based on semantic similarity.

    Args:
        query (str): The search query passed as a query parameter.

    Returns:
        Dict[str, Any]: A list of dictionaries containing matching news articles.
    """
    return controller.semantic_news_search(query)

@router.get("/question")
async def question_the_news(question: str = Query(..., description="The question the user what to ask")) -> Dict[str, Any]:
    """
    API route that receives a user question, and return an answer based on the relevant news.

    Args:
        question (str): The user's question to answer according the news.

    Returns:
        Dict[str, Any]: Dictionary with the relevant answer
    """
    return controller.question_the_news(question)