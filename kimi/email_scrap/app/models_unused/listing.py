"""
FCC Listing model
"""
from sqlalchemy import Column, String, Text, DateTime
from sqlalchemy.orm import relationship
from app.db.base import BaseModel


class Listing(BaseModel):
    """FCC Robocall Mitigation Database listing"""
    __tablename__ = "listings"
    
    # Core fields
    business_name = Column(String(500), nullable=True, index=True)
    frn = Column(String(50), unique=True, nullable=False, index=True)  # FCC Registration Number
    previous_business_names = Column(Text, nullable=True)
    business_address = Column(Text, nullable=True)
    other_dba_names = Column(Text, nullable=True)
    foreign_voice_service_provider = Column(String(500), nullable=True)
    implementation = Column(String(500), nullable=True)
    voice_service_provider = Column(String(500), nullable=True)
    gateway_provider = Column(String(500), nullable=True)
    non_gateway_intermediate_provider = Column(String(500), nullable=True)
    
    # Contact information
    robocall_mitigation_contact_name = Column(String(500), nullable=True)
    contact_title = Column(String(500), nullable=True)
    contact_department = Column(String(500), nullable=True)
    contact_business_address = Column(Text, nullable=True)
    contact_telephone_number = Column(String(100), nullable=True)
    attachment_link = Column(Text, nullable=True)  # ServiceNow URL
    sys_id = Column(String(100), nullable=True, index=True)  # ServiceNow sys_id
    
    # Metadata
    last_updated = Column(DateTime, nullable=True)
    
    # Relationships
    leads = relationship("Lead", back_populates="listing", cascade="all, delete-orphan")
