from pydantic_settings import BaseSettings


class TGConfig(BaseSettings):
    LOG_LEVEL: str = "INFO"

    RAG_HOST: str = "localhost"
    RAG_PORT: int = 8080
    RAG_N_RESULT: int = 5
    SENTENCE_TRANSFORMER_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"
    MISTRAL_API_KEY: str = "<KEY>"
    MISTRAL_API_MODEL: str = "mistral-7b"

    PYRO_API_ID: str = "<ID>"
    PYRO_API_HASH: str = "<HASH>"
    PYRO_HISTORY_LIMIT: int = 100

    MONGO_USERNAME: str = "<USERNAME>"
    MONGO_PASSWORD: str = "<PASSWORD>"
    MONGO_HOST: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_DATABASE_NAME: str = "<DATABASE>"

    AIOGRAM_API_KEY: str = "<KEY>"

    class Config:
        env_file = ".env"
        case_sensitive = False


def get_config() -> TGConfig:
    stn = TGConfig()
    print(stn.__dict__)
    return stn
