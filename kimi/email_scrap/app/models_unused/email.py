"""
Email model
"""
from sqlalchemy import Column, String, Text, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
import enum
from app.db.base import BaseModel


class EmailSource(str, enum.Enum):
    """Email source enumeration"""
    USA = "usa"
    NON_USA = "non_usa"
    LINKEDIN = "linkedin"
    MERGED = "merged"


class Email(BaseModel):
    """Scraped email model"""
    __tablename__ = "emails"
    
    # Foreign keys
    lead_id = Column(ForeignKey("leads.id", ondelete="SET NULL"), nullable=True, index=True)
    linkedin_lead_id = Column(ForeignKey("linkedin_leads.id", ondelete="SET NULL"), nullable=True, index=True)
    
    # Core fields
    company_name = Column(String(500), nullable=True, index=True)
    email = Column(String(255), nullable=False, index=True)
    
    # Source tracking
    source = Column(SQLEnum(EmailSource), nullable=False, index=True)
    
    # Metadata
    filing_url = Column(Text, nullable=True)  # URL where email was found
    error = Column(Text, nullable=True)
    
    # Relationships
    lead = relationship("Lead", back_populates="emails")
    linkedin_lead = relationship("LinkedInLead", back_populates="emails")
    verified_email = relationship("VerifiedEmail", back_populates="email", uselist=False, cascade="all, delete-orphan")
