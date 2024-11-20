class NewsLoadingError(Exception):
    """ Raised when there is an error loading the news data in the first run. """
    pass

class OpenAIEmbeddingError(Exception):
    """ Raised when there is an error in the request sent to OpenAI API embedding system"""
    pass

class OpenAIChatError(Exception):
    """ Raised when there is an error in the request sent to OpenAI ChatGPT-4o-mini chatbot"""
    pass

class SageMakerError(Exception):
    """ Raised when there is an error in the request sent to SageMaker"""
    pass