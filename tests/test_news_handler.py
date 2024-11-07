import pytest
from unittest.mock import MagicMock, patch
from handlers.news_handler import NewsHandler
from tests.data_mocks.test_news_handler_data_mock import SEMANTIC_NEWS_SEARCH_GET_ALL_ROWS_RESPONSE
from validators.news_validator import NewsQueryModel


@pytest.fixture
def mock_news_handler():
    handler = NewsHandler()
    handler.manager = MagicMock()
    return handler


def test_count_matching_news(mock_news_handler):
    """
    Test count_matching_news method to return correct count of matching news.
    """
    # Mock the output from count_matching_rows
    mock_news_handler.manager.count_matching_rows.return_value = 10
    query = NewsQueryModel(title="Test")

    result = mock_news_handler.count_matching_news(query)
    assert result == {"count": 10}
    mock_news_handler.manager.count_matching_rows.assert_called_once_with({
                                                                          "title": "Test"})


@patch('handlers.news_handler.embed_with_openai_batched')
def test_semantic_news_search(mock_embed, mock_news_handler):
    """
    Test semantic_news_search for returning news articles based on similarity.
    """
    # Mock embeddings and database response
    mock_embed.return_value = [[0.5, 0.2, 0.1]]
    mock_news_handler.manager.get_all_rows.return_value = SEMANTIC_NEWS_SEARCH_GET_ALL_ROWS_RESPONSE

    # Call method
    query = "relevant news"
    result = mock_news_handler.semantic_news_search(query)

    assert "related_news" in result
    assert len(result["related_news"]) == 2  # Now expects both items
    assert result["related_news"][0]["title"] == "Test News 1"
    assert result["related_news"][1]["title"] == "Test News 2"

    mock_news_handler.manager.get_all_rows.assert_called_once()
    mock_embed.assert_called_once_with([query])


@patch('handlers.news_handler.ask_chatgpt_4o_mini')
@patch.object(NewsHandler, 'semantic_news_search')
def test_question_the_news(mock_search, mock_ask, mock_news_handler):
    """
    Test question_the_news for generating answers based on relevant news.
    """
    # Mock semantic search results
    mock_search.return_value = {
        "related_news": [
            {"content": "Relevant content here"}
        ]
    }

    # Mock OpenAI answer generation
    mock_ask.return_value = "This is a generated answer based on the content provided."

    # Call method
    question = "What is the news?"
    result = mock_news_handler.question_the_news(question)

    assert result == {
        "answer": "This is a generated answer based on the content provided."}
    mock_search.assert_called_once_with(question)
    mock_ask.assert_called_once()
