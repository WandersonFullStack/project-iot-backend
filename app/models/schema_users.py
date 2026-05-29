from __future__ import annotations
from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import Optional, Literal
from datetime import datetime

class LoginIn(BaseModel):
    username: str
    password: str

class TokenOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 30

class AccessTokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"

class RefreshIn(BaseModel):
    refresh_token: str

class UserIn(BaseModel):
    username: str = Field(..., min_length=3, max_length=40, pattern=r"^[a-zA-Z0-9_\-]+$")
    email: EmailStr
    name: str = Field(..., min_length=2, max_length=80)
    password: str = Field(..., min_length=8, description="Minimum 8 characters")
    paper: Literal["admin", "operator", "read"] = "operator"

class UserOut(BaseModel):
    id: int
    username: str
    email: str
    name: str
    paper: str
    active: bool
    created_in: datetime
    login_only: Optional[datetime]

    model_config = {"from_attributes": True}

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    paper: Optional[Literal["admin", "operator", "read"]] = None
    active: Optional[bool] = None

class ReplacePasswordIn(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8)

    @field_validator("new_password")
    @classmethod
    def different_passwords(cls, value, info):
        if value == info.data.get("current_password"):
            raise ValueError("The new password must be different from the current one.")
        
        return value
    