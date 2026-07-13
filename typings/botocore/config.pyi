from typing import Any

class Config:
    signature_version: str | None

    def __init__(
        self,
        *,
        region_name: str | None = None,
        signature_version: str | None = None,
        **kwargs: Any,
    ) -> None: ...
