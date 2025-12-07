from pydantic import model_validator
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # AIOGRAM BOT
    BOT_TOKEN: str

    # MONGODB SETTINGS
    MONGO_HOST: str
    MONGO_PORT: int
    MONGO_USER: str
    MONGO_PASS: str
    MONGO_DB: str
    MONGO_URL: str = None

    @model_validator(mode="before")
    @classmethod
    def get_mongo_url(cls, v):
        v["MONGO_URL"] = "mongodb://" + \
            f"{v['MONGO_USER']}:{v['MONGO_PASS']}@" + \
            f"{v['MONGO_HOST']}:{v['MONGO_PORT']}/"
        return v

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings():
    return Settings()


settings = get_settings()
