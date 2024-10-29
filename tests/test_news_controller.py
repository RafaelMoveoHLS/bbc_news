import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from app import app
from controllers.news_controller import NewsController
from tests.data_mocks.test_news_controller_data_mock import SEMANTIC_NEWS_SEARCH_EMBED_WITH_OPENAI_BATCHED_RESPOSE, SEMANTIC_NEWS_SEARCH_GET_ALL_ROWS_RESPONSE, SEMANTIC_NEWS_SEARCH_RESPONSE

client = TestClient(app)  # Initialize the TestClient with the FastAPI app

# Fixture to mock the controller's handler


@pytest.fixture
def mock_news_handler():
    """
    Mock the NewsHandler instance used by NewsController.
    """
    news_controller = NewsController()
    news_controller.handler = MagicMock()
    return news_controller


def test_count_news_success(mock_news_handler, monkeypatch):
    """
    Test the /news/count route for success.

    Request Body:
        {"title": "Israel"}
    Expected Response:
        Status Code: 200
        Response Body: {"count": 233}
    """
    # Mock the handler to return a count of 233
    mock_news_handler.handler.count_matching_news.return_value = {"count": 233}

    # Use monkeypatch to replace the controller instance in the router with the mock
    monkeypatch.setattr(
        "routers.news_router.controller", mock_news_handler
    )

    # Make a POST request to the /news/count endpoint
    response = client.post("/news/count", json={"title": "Israel"})

    # Assert the status code is 200
    assert response.status_code == 200

    # Assert the response body is {"count": 233}
    assert response.json() == {"count": 233}


def test_count_news_failure(mock_news_handler, monkeypatch):
    """
    Test the /news/count route for failure.

    Request Body:
        {"title": "Israel"}
    Expected Response:
        Status Code: 500
        Error Message: "Error querying database"
    """
    # Mock the handler to raise an exception
    mock_news_handler.handler.count_matching_news.side_effect = Exception(
        "Database error")

    # Use monkeypatch to replace the controller instance in the router with the mock
    monkeypatch.setattr(
        "routers.news_router.controller", mock_news_handler
    )

    # Make a POST request to the /news/count endpoint
    response = client.post("/news/count", json={"title": "Israel"})

    # Assert the status code is 500
    assert response.status_code == 500

    # Assert the error message in the response
    assert "Error querying database" in response.json()["detail"]


@patch("handlers.news_handler.embed_with_openai_batched")
@patch("handlers.news_handler.NewsHandler.manager.get_all_rows")
def test_semantic_news_search_success(mock_get_all_rows, mock_embed_with_openai_batched, mock_news_handler, monkeypatch):
    """
    Test the /news/search route for success.
    """
    # Mock the handler to return a list of related news
    mock_news_handler.handler.semantic_news_search.return_value = SEMANTIC_NEWS_SEARCH_RESPONSE

    # Mock the return value of the get_all_rows method
    mock_get_all_rows.return_value = SEMANTIC_NEWS_SEARCH_GET_ALL_ROWS_RESPONSE

    # Mock the return value of the embed_with_openai_batched function
    mock_embed_with_openai_batched.return_value = SEMANTIC_NEWS_SEARCH_EMBED_WITH_OPENAI_BATCHED_RESPOSE

    # Use monkeypatch to replace the controller instance in the router with the mock
    monkeypatch.setattr("routers.news_router.controller", mock_news_handler)
    response = client.get("/news/search", params={"query": "tech innovation"})
    assert response.status_code == 200
    assert response.json() == SEMANTIC_NEWS_SEARCH_RESPONSE

@patch("handlers.news_handler.embed_with_openai_batched")
@patch("handlers.news_handler.NewsHandler.manager.get_all_rows")
def test_semantic_news_search_failure(mock_get_all_rows, mock_embed_with_openai_batched, mock_news_handler, monkeypatch):
    """
    Test the /news/search route for failure.

    Query Parameter:
        query: "tech innovation"
    Expected Response:
        Status Code: 500
        Error Message: "Error while searching the news"
    """
    # Mock the return value of the embed_with_openai_batched function
    mock_embed_with_openai_batched.return_value = SEMANTIC_NEWS_SEARCH_EMBED_WITH_OPENAI_BATCHED_RESPOSE

    # Mock the return value of the get_all_rows method
    mock_get_all_rows.return_value = SEMANTIC_NEWS_SEARCH_GET_ALL_ROWS_RESPONSE

    # Mock the handler to raise an exception
    mock_news_handler.handler.semantic_news_search.side_effect = Exception("Database error")

    # Use monkeypatch to replace the controller instance in the router with the mock
    monkeypatch.setattr("routers.news_router.controller", mock_news_handler)

    # Make a GET request to the /news/search endpoint
    response = client.get("/news/search", params={"query": "tech innovation"})

    # Assert the status code is 500
    assert response.status_code == 500

    # Assert the error message in the response
    assert "Error while searching the news" in response.json()["detail"]