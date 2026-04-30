"""
LinkedIn Lead model
"""
from sqlalchemy import Column, String, Text, JSON
from sqlalchemy.orm import relationship
from app.db.base import BaseModel


class LinkedInLead(BaseModel):
    """LinkedIn scraped lead model"""
    __tablename__ = "linkedin_leads"
    
    # Core fields from Apify scraping
    name = Column(String(500), nullable=True, index=True)
    company = Column(String(500), nullable=True, index=True)
    title = Column(String(500), nullable=True)
    location = Column(String(500), nullable=True)
    profile_url = Column(Text, nullable=True)
    company_url = Column(String(500), nullable=True)
    
    # Additional fields (stored as JSON for flexibility)
    additional_data = Column(JSON, nullable=True)
    
    # Search metadata
    search_keyword = Column(String(500), nullable=True, index=True)
    apify_run_id = Column(String(100), nullable=True, index=True)
    
    # Relationships
    emails = relationship("Email", back_populates="linkedin_lead", cascade="all, delete-orphan")
