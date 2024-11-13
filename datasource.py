from enum import Enum
from typing import Dict

from pydantic import BaseModel


class DataSourcesSupported(str, Enum):
    JSON = "json"
    CSV = "csv"

    def __str__(self) -> str:
        return self.value


class DataSource(BaseModel):
    domain: str
    type: str
    config: Dict[str, str]

