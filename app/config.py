from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- Telegram / MTProto credentials ---
    tg_api_id: int = Field(..., description="From https://my.telegram.org")
    tg_api_hash: str = Field(..., min_length=1)
    tg_bot_token: str = Field(..., min_length=1)

    # --- Storage locations ---
    data_dir: Path = Field(default=Path("./data"))

    # --- gallery-dl pacing ---
    gdl_sleep_min: float = 1.5
    gdl_sleep_max: float = 4.0
    gdl_sleep_request: str = "1-3"
    gdl_limit_rate: str = "3M"
    gdl_retries: int = 4
    gdl_run_timeout_s: int = 3 * 3600

    # --- adaptive backoff on rate-limit signals ---
    gdl_max_run_retries: int = 3
    gdl_backoff_base_s: float = 30.0
    gdl_backoff_multiplier: float = 2.5

    # --- Telegram upload pacing ---
    tg_upload_delay_min: float = 2.0
    tg_upload_delay_max: float = 4.5
    tg_batch_size: int = 30
    tg_batch_cooldown_s: float = 25.0
    tg_upload_max_retries: int = 3
    tg_max_concurrent_uploads: int = 1  # keep at 1 unless you know Telegram tolerates more

    # --- misc ---
    max_upload_bytes: int = 2 * 1024 * 1024 * 1024  # 2GB, MTProto ceiling
    progress_edit_every_n: int = 25
    log_level: str = "INFO"
    log_dir: Path = Field(default=Path("./logs"))

    @field_validator("data_dir", "log_dir")
    @classmethod
    def _ensure_dir(cls, v: Path) -> Path:
        v.mkdir(parents=True, exist_ok=True)
        return v

    @property
    def db_path(self) -> Path:
        return self.data_dir / "state.sqlite3"

    @property
    def gdl_archive_path(self) -> Path:
        return self.data_dir / "gdl_archive.sqlite3"

    @property
    def downloads_dir(self) -> Path:
        d = self.data_dir / "downloads"
        d.mkdir(parents=True, exist_ok=True)
        return d


settings = Settings()  # type: ignore[call-arg]  # populated from env/.env at import time
