"""
Application configuration settings
"""
from pydantic_settings import BaseSettings
from typing import List, Optional
import os
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """Application settings"""
    
    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "crwm_db")
    DB_USER: str = os.getenv("DB_USER", "crwm_user")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "crwm_password")
    
    @property
    def DATABASE_URL(self) -> str:
        """Construct database URL"""
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    @property
    def DATABASE_URL_SYNC(self) -> str:
        """Construct synchronous database URL for Alembic"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # Application Settings
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # Session Configuration
    SESSION_SECRET_KEY: str = os.getenv("SESSION_SECRET_KEY", "your-session-secret-key-change-in-production")
    
    # Apify API Keys
    APIFY_API_KEY_1: Optional[str] = os.getenv("APIFY_API_KEY_1")
    APIFY_API_KEY_2: Optional[str] = os.getenv("APIFY_API_KEY_2")
    APIFY_API_KEY_3: Optional[str] = os.getenv("APIFY_API_KEY_3")
    APIFY_API_KEY_4: Optional[str] = os.getenv("APIFY_API_KEY_4")
    APIFY_API_KEY_5: Optional[str] = os.getenv("APIFY_API_KEY_5")
    
    @property
    def APIFY_API_KEYS(self) -> List[str]:
        """Get list of available Apify API keys"""
        keys = [
            self.APIFY_API_KEY_1,
            self.APIFY_API_KEY_2,
            self.APIFY_API_KEY_3,
            self.APIFY_API_KEY_4,
            self.APIFY_API_KEY_5,
        ]
        return [key for key in keys if key]
    
    # Email Verification API
    EMAIL_VERIFICATION_API_URL: str = os.getenv(
        "EMAIL_VERIFICATION_API_URL",
        "https://rapid-email-verifier.fly.dev/api/validate"
    )
    
    # FCC URLs
    FCC_LISTINGS_URL: str = "https://fccprod.servicenowservices.com/rmd?id=rmd_listings"
    FCC_FORM_499_URL: str = "https://apps.fcc.gov/cgb/form499/499results.cfm"
    
    # CORS Settings
    CORS_ORIGINS: List[str] = ["*"]  # Configure appropriately for production
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
