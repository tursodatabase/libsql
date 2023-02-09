from typing import Optional

class ClientError(RuntimeError):
    pass

class ClientResponseError(ClientError):
    pass

class ClientHttpError(ClientError):
    def __init__(self, status: int, message: Optional[str]):
        self.status = status
        self.message = message

    def __str__(self) -> str:
        return f"HTTP status {self.status}, message={self.message!r}"
