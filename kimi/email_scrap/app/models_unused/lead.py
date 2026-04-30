"""
Lead model (USA/non-USA leads)
"""
from sqlalchemy import Column, String, Text, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
import enum
from app.db.base import BaseModel


class LeadType(str, enum.Enum):
    """Lead type enumeration"""
    USA = "usa"
    NON_USA = "non_usa"


class Lead(BaseModel):
    """Lead model for USA and non-USA leads"""
    __tablename__ = "leads"
    
    # Foreign key
    listing_id = Column(ForeignKey("listings.id", ondelete="CASCADE"), nullable=True, index=True)
    
    # Core fields
    frn = Column(String(50), nullable=False, index=True)
    sys_id = Column(String(100), nullable=True, index=True)
    company_name = Column(String(500), nullable=True)
    service_type = Column(String(500), nullable=True)
    contact_email = Column(String(255), nullable=True)
    contact_phone = Column(String(100), nullable=True)
    website = Column(String(500), nullable=True)
    
    # FCC Form 499 verification fields
    fcc_499_status = Column(String(100), nullable=True)
    filer_id = Column(String(100), nullable=True)
    legal_name_499 = Column(String(500), nullable=True)
    dba_499 = Column(String(500), nullable=True)
    verification_link = Column(Text, nullable=True)
    
    # Lead type
    lead_type = Column(SQLEnum(LeadType), nullable=False, index=True)
    
    # Error tracking
    error = Column(Text, nullable=True)
    
    # Relationships
    listing = relationship("Listing", back_populates="leads")
    emails = relationship("Email", back_populates="lead", cascade="all, delete-orphan")
