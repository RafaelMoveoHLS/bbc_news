from pydantic import BaseModel, Field, model_validator
from typing import Optional, Dict

class QueryModel(BaseModel):
    """
    Pydantic model to validate the incoming JSON payload, ensuring that
    at least one of the specified fields is provided and all values are strings.
    """
    title: Optional[str] = Field(None)
    pubDate: Optional[str] = Field(None)
    guid: Optional[str] = Field(None)
    link: Optional[str] = Field(None)
    description: Optional[str] = Field(None)

    @model_validator(mode="before")
    def check_at_least_one_field(cls, values: Dict[str, Optional[str]]):
        """
        Validator that ensures that at least one of the fields ('title', 'pubDate', 'guid', 'link', 'description') 
        is provided in the request payload and that all values are strings.

        Args:
            values (Dict[str, Optional[str]]): A dictionary of the field values provided in the request payload.

        Returns:
            Dict[str, Optional[str]]: The validated field values, ensuring they conform to the validation rules.
        """
        required_keys = ['title', 'pubDate', 'guid', 'link', 'description']

        # Check if at least one of these keys is provided
        if not any(values.get(key) for key in required_keys):
            raise ValueError(f"At least one of the fields {required_keys} must be provided.")

        # Ensure all provided values are strings
        for key, value in values.items():
            if value is not None and not isinstance(value, str):
                raise ValueError(f"The field '{key}' must be a string.")

        return values
