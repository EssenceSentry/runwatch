from typing import Any

class ClientError(Exception):
    response: Any
    operation_name: str

    def __init__(self, error_response: dict[str, Any], operation_name: str) -> None: ...
