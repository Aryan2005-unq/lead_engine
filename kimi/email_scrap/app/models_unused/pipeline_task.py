"""
Pipeline Task model
"""
from sqlalchemy import Column, String, Text, ForeignKey, Enum as SQLEnum, DateTime, JSON, Integer
from sqlalchemy.orm import relationship
import enum
from app.db.base import BaseModel


class TaskStatus(str, enum.Enum):
    """Task status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class PipelineTask(BaseModel):
    """Individual task within a pipeline run"""
    __tablename__ = "pipeline_tasks"
    
    # Foreign key
    pipeline_run_id = Column(ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Core fields
    task_name = Column(String(255), nullable=False, index=True)
    task_type = Column(String(100), nullable=False)  # e.g., "update_listings", "verify_leads", etc.
    status = Column(SQLEnum(TaskStatus), default=TaskStatus.PENDING, nullable=False, index=True)
    
    # Execution metadata
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    error_traceback = Column(Text, nullable=True)
    
    # Task configuration and results
    config = Column(JSON, nullable=True)
    result = Column(JSON, nullable=True)  # Store task results
    
    # Progress tracking
    progress_percentage = Column(Integer, default=0, nullable=False)
    progress_message = Column(String(500), nullable=True)
    
    # Relationships
    pipeline_run = relationship("PipelineRun", back_populates="tasks")
