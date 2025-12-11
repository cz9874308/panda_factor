"""
聊天数据模型模块

本模块定义了聊天相关的数据模型，包括消息、会话和请求/响应模型。
使用 Pydantic 进行数据验证和序列化。

核心概念
--------

- **消息模型**：定义单条消息的数据结构
- **会话模型**：定义聊天会话的数据结构
- **请求/响应模型**：定义 API 请求和响应的数据结构

为什么需要这个模块？
-------------------

在聊天服务中，需要定义数据结构：
- 需要定义消息的格式
- 需要定义会话的格式
- 需要定义 API 请求和响应的格式

这个模块提供了这些数据模型。

注意事项
--------

- 使用 Pydantic 进行数据验证
- 所有时间戳都使用 ISO 格式
- 提供了示例数据用于 API 文档
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class Message(BaseModel):
    """消息模型

    这个类定义了单条消息的数据结构。

    Attributes:
        role: 消息角色，'user' 或 'assistant'
        content: 消息内容
        timestamp: 消息时间戳，默认当前时间
    """
    role: str  # user 或 assistant
    content: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class ChatSession(BaseModel):
    """聊天会话模型

    这个类定义了聊天会话的数据结构。

    Attributes:
        id: 会话 ID
        user_id: 用户 ID
        messages: 消息列表，默认空列表
        created_at: 创建时间，默认当前时间
        updated_at: 更新时间，默认当前时间
    """
    id: str
    user_id: str
    messages: List[Message] = Field(default_factory=list)
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now().isoformat())

    class Config:
        schema_extra = {
            "example": {
                "user_id": "user123",
                "messages": [
                    {
                        "role": "user",
                        "content": "你好",
                        "timestamp": datetime.now().isoformat()
                    },
                    {
                        "role": "assistant",
                        "content": "你好！有什么我可以帮你的吗？",
                        "timestamp": datetime.now().isoformat()
                    }
                ],
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
        }

class ChatRequest(BaseModel):
    """聊天请求模型

    这个类定义了聊天请求的数据结构。

    Attributes:
        user_id: 用户 ID
        message: 用户消息
        session_id: 会话 ID（可选）
    """
    user_id: str
    message: str
    session_id: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "user_id": "user123",
                "message": "你好",
                "session_id": "optional_session_id"
            }
        }

class ChatResponse(BaseModel):
    """聊天响应模型

    这个类定义了聊天响应的数据结构。

    Attributes:
        session_id: 会话 ID
        message: AI 回答
        timestamp: 响应时间戳
    """
    session_id: str
    message: str
    timestamp: str

    class Config:
        schema_extra = {
            "example": {
                "session_id": "session123",
                "message": "你好！有什么我可以帮你的吗？",
                "timestamp": datetime.now().isoformat()
            }
        } 