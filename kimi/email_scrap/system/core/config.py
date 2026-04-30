"""
Pipeline System Configuration
Reads all settings from environment variables with sensible defaults.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class PipelineConfig:
    """Central configuration for the queue-based pipeline system."""

    # ── Database (PostgreSQL via asyncpg) ──
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "crwm_db")
    DB_USER: str = os.getenv("DB_USER", "crwm_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "crwm_password")
    DB_POOL_MIN: int = int(os.getenv("DB_POOL_MIN", "2"))
    DB_POOL_MAX: int = int(os.getenv("DB_POOL_MAX", "10"))

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    # ── Redis ──
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # ── Queue names ──
    Q_NORMALIZE = "pipeline:normalize_queue"
    Q_VERIFY = "pipeline:verify_queue"
    Q_ENRICH = "pipeline:enrich_queue"
    Q_RETRY = "pipeline:retry_queue"
    Q_DEAD = "pipeline:dead_letter_queue"

    # ── Batch sizes ──
    INTAKE_BATCH: int = int(os.getenv("INTAKE_BATCH", "200"))
    NORMALIZE_BATCH: int = int(os.getenv("NORMALIZE_BATCH", "200"))
    VERIFY_BATCH: int = int(os.getenv("VERIFY_BATCH", "100"))
    ENRICH_BATCH: int = int(os.getenv("ENRICH_BATCH", "50"))
    RETRY_BATCH: int = int(os.getenv("RETRY_BATCH", "50"))

    # ── Concurrency limits ──
    VERIFY_CONCURRENCY: int = int(os.getenv("VERIFY_CONCURRENCY", "50"))
    ENRICH_CONCURRENCY: int = int(os.getenv("ENRICH_CONCURRENCY", "3"))
    RETRY_CONCURRENCY: int = int(os.getenv("RETRY_CONCURRENCY", "10"))

    # ── Retry policy ──
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_BACKOFF_BASE: float = float(os.getenv("RETRY_BACKOFF_BASE", "2.0"))

    # ── Worker loop sleep (seconds) when queue is empty ──
    IDLE_SLEEP: float = float(os.getenv("IDLE_SLEEP", "5.0"))

    # ── FCC URLs ──
    FCC_LISTINGS_URL: str = "https://fccprod.servicenowservices.com/rmd?id=rmd_listings"
    FCC_FORM_499_URL: str = "https://apps.fcc.gov/cgb/form499/499results.cfm"

    # ── Playwright ──
    BROWSER_TIMEOUT: int = int(os.getenv("BROWSER_TIMEOUT", "30000"))


config = PipelineConfig()
