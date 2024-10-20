import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from services.dataloader import load_data
from services.exeptions import NewsLoadingError

@pytest.mark.skip(reason="Not ready yet")
@patch("services.dataloader.pd.read_csv")
@patch("services.dataloader.embed_with_openai_batched")
def test_load_data_when_db_empty(
    mock_read_csv,
    mock_embed_with_openai_batched,
    monkeypatch
):
    """
    Test the load_data function when the database is empty, and data is successfully loaded.
    """
    # Mock the NewsManager instance
    mock_news_manager = MagicMock()
    mock_news_manager.collection.count_documents.return_value = 0

    # Monkeypatch the news_manager instance in the dataloader module
    monkeypatch.setattr("services.dataloader.news_manager", mock_news_manager)

    # Create a mock DataFrame with test data
    mock_df = pd.DataFrame({
        'title': ['News 1', 'News 2', 'News 3'],
        'pubDate': ['2024-03-07', '2022-03-07', '2024-03-06'],
        'guid': ['guild 1', 'guild 2', 'guild 3'],
        'link': ['link 1', 'link 2', 'link 3'],
        'description': ['Description 1', 'Description 2', 'Description 3']
    })
    mock_read_csv.return_value = mock_df
    print(mock_read_csv.return_value)

    # Mock the embeddings
    mock_embed_with_openai_batched.return_value = [[0.1, 0.1], [0.2, 0.2]]

    # Call the load_data function
    load_data()

    # Assertions
    mock_read_csv.assert_called_once_with('bbc_news_data/bbc_news.csv')
    mock_news_manager.insert_many.assert_called_once_with([
        {
            'title': 'News 1',
            'pubDate': pd.Timestamp('2024-03-07'),
            'guid': 'guild 1',
            'link': 'link 1',
            'description': 'Description 1',
            'openai_embedding': [0.1, 0.1]
        },
        {
            'title': 'News 3',
            'pubDate': pd.Timestamp('2024-03-06'),
            'guid': 'guild 3',
            'link': 'link 3',
            'description': 'Description 3',
            'openai_embedding': [0.2, 0.2]
        }
    ])


@pytest.mark.skip(reason="Not ready yet")
@patch("services.dataloader.pd.read_csv")
def test_load_data_collection_not_empty(
    mock_read_csv,
    monkeypatch
):
    """
    Test the load_data function when the MongoDB collection is not empty.
    """
    # Mock the NewsManager instance
    mock_news_manager = MagicMock()
    mock_news_manager.collection.count_documents.return_value = 5

    # Monkeypatch the news_manager instance in the dataloader module
    monkeypatch.setattr("services.dataloader.news_manager", mock_news_manager)

    # Call the load_data function
    load_data()

    # Assertions
    mock_read_csv.assert_not_called()
    mock_news_manager.insert_many.assert_not_called()