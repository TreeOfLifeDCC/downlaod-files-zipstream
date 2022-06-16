from typing import List, Optional

from pydantic.main import BaseModel


class Taxonomy(BaseModel):
    rank: Optional[str]
    taxonomy: Optional[str]
    childRank: Optional[str]
    commonName: Optional[str]
    Other: Optional[str]


# Pydantic BaseModel
# Order class model for request body
class Request(BaseModel):
    downloadOption: str
    taxonomyFilter: List[Taxonomy]

