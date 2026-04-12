"""Application configuration via environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Google Sheets
    google_sheets_credentials_json: str
    spreadsheet_id: str = "1QMlpX4rdM77NIVXlhU9XWRPQEeotshe64TTqT9zzxFM"

    # Firebase Admin
    firebase_project_id: str
    firebase_private_key: str
    firebase_client_email: str

    # CORS
    allowed_origins: str = "http://localhost:3000"

    @property
    def origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",")]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
