from pathlib import Path
from pydantic import BaseSettings, Field
from typing import Optional

class Settings(BaseSettings):
    base_url: str = Field(default="https://data.elexon.co.uk/bmrs/api/v1")
    timeout_s: float = 30.0
    max_retries: int = 3
    backoff_base: float = 0.5
    backoff_cap: float = 5.0
    max_concurrency: int = 128
    rate_per_sec: float = 80.0
    user_agent: str = "elexon-dl/0.2"
    cache_enabled: bool = True
    cache_dir: Optional[str] = str(Path("~/.cache/elexon-dl/http").expanduser())
    cache_ttl_s: int = 0  # 0 => never expire
    health_url: str = "https://data.elexon.co.uk/bmrs/api/v1/health"

    model_config = {"env_prefix": "ELEXON_", "extra": "ignore"}