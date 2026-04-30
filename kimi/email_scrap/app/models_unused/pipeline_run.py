"""
Pipeline Run model
"""
from sqlalchemy import Column, String, Text, Enum as SQLEnum, DateTime, JSON, Integer
from sqlalchemy.orm import relationship
import enum
from app.db.base import BaseModel


class PipelineStatus(str, enum.Enum):
    """Pipeline status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineRun(BaseModel):
    """Pipeline execution run model"""
    __tablename__ = "pipeline_runs"
    
    # Core fields
    name = Column(String(255), nullable=False, index=True)
    status = Column(SQLEnum(PipelineStatus), default=PipelineStatus.PENDING, nullable=False, index=True)
    
    # Execution metadata
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Configuration
    config = Column(JSON, nullable=True)  # Store pipeline configuration
    
    # Statistics
    total_tasks = Column(Integer, default=0, nullable=False)
    completed_tasks = Column(Integer, default=0, nullable=False)
    failed_tasks = Column(Integer, default=0, nullable=False)
    
    # Relationships
    tasks = relationship("PipelineTask", back_populates="pipeline_run", cascade="all, delete-orphan")
