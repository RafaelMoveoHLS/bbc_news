import pytest
from unittest.mock import MagicMock, patch
from managers.news_manager import NewsManager

@pytest.fixture
def mock_news_manager():
    """
    Fixture to create a NewsManager instance with mocked db and collection.
    """
    with patch('managers.main_manager.MongoClient') as mock_client:
        # Mock the MongoDB database and collection
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.return_value.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection

        # Initialize the NewsManager with mocked components
        manager = NewsManager()
        return manager, mock_collection

def test_count_matching_rows(mock_news_manager):
    """
    Test the count_matching_rows method to ensure it returns the correct count.
    """
    manager, mock_collection = mock_news_manager

    # Set up the mock to return 5 matching documents
    mock_collection.count_documents.return_value = 5

    # Test query
    query = {"title": "Israel"}
    result = manager.count_matching_rows(query)

    # Assertions
    mock_collection.count_documents.assert_called_once_with(
        {"title": {"$regex": "Israel", "$options": "i"}}
    )
    assert result == 5

def test_insert_many(mock_news_manager):
    """
    Test the insert_many method to ensure documents are inserted correctly.
    """
    manager, mock_collection = mock_news_manager

    # Test data
    data = [{"title": "News 1"}, {"title": "News 2"}]

    # Call the method
    manager.insert_many(data)

    # Assertions
    mock_collection.insert_many.assert_called_once_with(data)
