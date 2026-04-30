"""
Activity Log model
"""
from sqlalchemy import Column, String, Text, Integer, ForeignKey, JSON, DateTime
from sqlalchemy.orm import relationship
from app.db.base import BaseModel


class ActivityLog(BaseModel):
    """Activity log model for tracking system activities"""
    __tablename__ = "activity_logs"
    
    # Foreign key
    user_id = Column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    
    # Core fields
    user_email = Column(String(255), nullable=True, index=True)
    action = Column(String(100), nullable=False, index=True)
    resource_type = Column(String(100), nullable=True, index=True)
    resource_id = Column(String(255), nullable=True, index=True)
    
    # Details and status
    details = Column(JSON, nullable=True)
    status = Column(String(20), default="success", nullable=False, index=True)
    error_message = Column(Text, nullable=True)
    error_traceback = Column(Text, nullable=True)
    
    # IP address
    ip_address = Column(String(45), nullable=True, index=True)
    
    # Timestamp override (if needed)
    logged_at = Column(DateTime, nullable=True)
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id])
