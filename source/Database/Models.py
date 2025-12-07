from typing import List, Optional
from pydantic import BaseModel, Field


class UserModel(BaseModel):
    id: int = Field(alias="_id")
    name: str
    channels: List[int] = Field(default_factory=list)

    class Config:
        populate_by_name = True


class ChannelModel(BaseModel):
    id: int = Field(alias="_id")
    name: str
    subscribers: int = 0

    class Config:
        populate_by_name = True
