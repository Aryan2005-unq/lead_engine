"""
Verified Email model
"""
from sqlalchemy import Column, String, ForeignKey, Boolean, DateTime
from sqlalchemy.orm import relationship
from app.db.base import BaseModel


class VerifiedEmail(BaseModel):
    """Verified email model"""
    __tablename__ = "verified_emails"
    
    # Foreign key
    email_id = Column(ForeignKey("emails.id", ondelete="CASCADE"), unique=True, nullable=False, index=True)
    
    # Verification fields
    is_valid = Column(Boolean, default=False, nullable=False)
    is_deliverable = Column(Boolean, default=False, nullable=False)
    verification_status = Column(String(100), nullable=True)
    
    # Verification metadata
    verified_at = Column(DateTime, nullable=True)
    verification_api_response = Column(String(500), nullable=True)  # Store API response summary
    
    # Relationships
    email = relationship("Email", back_populates="verified_email")
