"""
User model
"""
from sqlalchemy import Column, String, Text
from app.db.base import BaseModel


class User(BaseModel):
    """User account model"""
    __tablename__ = "users"
    
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(Text, nullable=False)
