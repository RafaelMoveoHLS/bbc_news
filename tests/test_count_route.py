import pytest
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

def test_count_route_with_real_db():
    """
    Test the /count route by sending a POST request with a "title" field,
    and expect the correct count based on real MongoDB data.
    """
    # Define the request body
    request_body = {
        "title": "Israel"
    }

    # Send a POST request to the /count route
    response = client.post("/count", json=request_body)

    # Assert the status code is 200
    assert response.status_code == 200

    # Assert the response body contains the expected count
    assert response.json() == {"count": 233}