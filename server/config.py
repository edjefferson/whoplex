from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Stored as comma-separated strings — pydantic-settings won't JSON-decode plain str fields
    media_dirs: str = ""
    media_extensions: str = ".mkv,.mp4,.avi,.m4v,.mov"

    db_path: Path = Path("data/library.db")
    thumb_dir: Path = Path("data/thumbs")
    base_url: str = "http://raspberrypi.local:8000"
    scan_on_startup: bool = True
    thumb_seek_seconds: float = 180.0
    host: str = "0.0.0.0"
    port: int = 8000

    @property
    def media_dirs_list(self) -> list[str]:
        return [p.strip() for p in self.media_dirs.split(",") if p.strip()]

    @property
    def media_extensions_set(self) -> set[str]:
        return {e.strip().lower() for e in self.media_extensions.split(",") if e.strip()}

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
