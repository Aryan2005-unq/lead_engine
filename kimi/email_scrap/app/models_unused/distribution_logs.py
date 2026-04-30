"""
SQLAlchemy models for Lead Distribution Engine
"""
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Enum as SQLEnum
from sqlalchemy.orm import relationship
import enum
from datetime import datetime
from app.db.base import Base

# ============================================
# Enumerations
# ============================================

class LeadStatus(str, enum.Enum):
    UNASSIGNED = "unassigned"
    ASSIGNED = "assigned"
    VERIFIED = "verified"

class MemberRole(str, enum.Enum):
    ADMIN = "admin"
    MEMBER = "member"


# ============================================
# Models
# ============================================

class Company(Base):
    """Companies Table Model"""
    __tablename__ = "companies_dist"  # Kept suffix to avoid overlaps if any pre-existing table setup
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    members = relationship("Member", back_populates="company", cascade="all, delete-orphan")
    leads = relationship("EmailLead", back_populates="company")


class Member(Base):
    """Members Table Model"""
    __tablename__ = "members_dist"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True, nullable=False, index=True)
    company_id = Column(Integer, ForeignKey("companies_dist.id", ondelete="CASCADE"), nullable=False, index=True)
    role = Column(SQLEnum(MemberRole), default=MemberRole.MEMBER, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    company = relationship("Company", back_populates="members")
    leads = relationship("EmailLead", back_populates="member")


class EmailLead(Base):
    """Email Leads Table Model"""
    __tablename__ = "email_leads_dist"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), nullable=False, index=True)
    status = Column(SQLEnum(LeadStatus), default=LeadStatus.UNASSIGNED, nullable=False, index=True)
    company_id = Column(Integer, ForeignKey("companies_dist.id", ondelete="SET NULL"), nullable=True, index=True)
    assigned_member_id = Column(Integer, ForeignKey("members_dist.id", ondelete="SET NULL"), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Relationships
    company = relationship("Company", back_populates="leads")
    member = relationship("Member", back_populates="leads")


class DistributionLog(Base):
    """Distribution Logs Table Model"""
    __tablename__ = "distribution_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, ForeignKey("companies_dist.id", ondelete="CASCADE"), nullable=False, index=True)
    member_id = Column(Integer, ForeignKey("members_dist.id", ondelete="CASCADE"), nullable=False, index=True)
    lead_count = Column(Integer, nullable=False)
    distributed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
