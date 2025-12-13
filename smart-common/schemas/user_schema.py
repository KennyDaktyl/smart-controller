from datetime import datetime
from typing import List

from pydantic import EmailStr

from app.constans.role import UserRole
from app.schemas.base import APIModel, ORMModel
from app.schemas.installation_schema import InstallationOut


class UserCreate(APIModel):
    email: EmailStr
    password: str


class UserLogin(APIModel):
    email: EmailStr
    password: str


class TokenResponse(APIModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class UserResponse(ORMModel):
    id: int
    email: EmailStr
    role: UserRole
    is_active: bool
    created_at: datetime
    huawei_username: str | None = None


class HuaweiCredentialsUpdate(APIModel):
    huawei_username: str
    huawei_password: str


class UserInstallationsResponse(ORMModel):
    id: int
    email: str
    role: str
    huawei_username: str | None = None
    created_at: datetime
    installations: List[InstallationOut] = []


class EmailTokenRequest(APIModel):
    token: str


class PasswordResetRequest(APIModel):
    email: EmailStr


class PasswordResetConfirm(APIModel):
    token: str
    new_password: str


class MessageResponse(APIModel):
    message: str
