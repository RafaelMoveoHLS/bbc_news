import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from app import app
from controllers.news_controller import NewsController

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
    mock_news_handler.handler.count_matching_news.side_effect = Exception("Database error")

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
