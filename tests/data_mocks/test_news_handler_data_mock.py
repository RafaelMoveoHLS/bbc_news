from datetime import datetime


SEMANTIC_NEWS_SEARCH_GET_ALL_ROWS_RESPONSE = [
    {
        "title": "Test News 1",
        "openai_embedding": [0.5, 0.2, 0.1],
        "description": "Details",
        "link": "url1",
        "pubDate": datetime(2024, 1, 1),  # Use datetime instead of string
        "guid": "guid1",
        "content": "Content 1"
    },
    {
        "title": "Test News 2",
        "openai_embedding": [0.1, 0.4, 0.3],
        "description": "More details",
        "link": "url2",
        "pubDate": datetime(2024, 1, 2),  # Use datetime instead of string
        "guid": "guid2",
        "content": "Content 2"
    }
]
