from dataclasses import dataclass
from datetime import date
from typing import Optional
@dataclass
class UserHistory:
    userid: int
    date : date
    hasOrdered: bool
    finalPrice: Optional[float]


