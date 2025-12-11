"""
聊天路由模块

本模块提供了聊天相关的 API 路由，包括发送消息、获取会话列表等功能。
它使用 FastAPI 构建 RESTful API，支持流式响应。

核心概念
--------

- **聊天接口**：处理用户发送的聊天消息
- **流式响应**：使用 Server-Sent Events (SSE) 实现流式返回
- **会话管理**：支持获取用户的聊天会话列表

为什么需要这个模块？
-------------------

在 Web 应用中，需要提供 API 接口：
- 前端需要发送聊天消息
- 前端需要获取 AI 回答
- 前端需要查看历史会话

这个模块提供了这些 API 接口。

工作原理（简单理解）
------------------

就像 API 网关：

1. **接收请求**：接收前端的 HTTP 请求（就像接收请求）
2. **处理业务**：调用服务层处理业务逻辑（就像处理业务）
3. **返回响应**：返回处理结果（就像返回响应）

注意事项
--------

- 使用 Server-Sent Events (SSE) 实现流式响应
- 所有错误都会返回适当的 HTTP 状态码
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import json

from panda_llm.services.chat_service import ChatService

router = APIRouter()
chat_service = ChatService()

class ChatRequest(BaseModel):
    """聊天请求模型

    这个类定义了聊天请求的数据结构。

    Attributes:
        user_id: 用户 ID
        message: 用户消息
        session_id: 会话 ID（可选，如果不存在则创建新会话）
    """
    user_id: str
    message: str
    session_id: Optional[str] = None

@router.post("/chat")
async def chat(request: ChatRequest):
    """处理聊天请求（流式响应）

    这个接口处理用户的聊天请求，并以流式方式返回 AI 的回答。
    使用 Server-Sent Events (SSE) 实现流式响应。

    为什么需要这个接口？
    --------------------

    前端需要发送聊天消息并获取回答：
    - 需要支持实时显示回答生成过程
    - 需要提供类似 ChatGPT 的交互体验

    这个接口提供了流式聊天功能。

    Args:
        request: 聊天请求，包含用户 ID、消息和会话 ID

    Returns:
        StreamingResponse: 流式响应，使用 SSE 格式

    Raises:
        HTTPException: 如果处理消息失败，返回 500 错误

    Example:
        >>> POST /llm/chat
        >>> {
        ...     "user_id": "user1",
        ...     "message": "如何计算动量因子？",
        ...     "session_id": "session123"
        ... }
    """
    try:
        # 使用流式处理
        async def generate():
            try:
                # async for chunk in chat_service.process_message_stream(
                async for chunk in chat_service.process_message_stream(
                    request.user_id,
                    request.message,
                    request.session_id
                ):
                    yield f"data: {json.dumps({'content': chunk})}\n\n"
            except ValueError as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': '处理消息时发生错误'})}\n\n"
            finally:
                yield "data: [DONE]\n\n"

        return StreamingResponse(
            generate(),
            media_type="text/event-stream"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/chat/sessions")
async def get_sessions(user_id: str, limit: int = 10):
    """获取用户的聊天会话列表

    这个接口获取指定用户的所有聊天会话列表。

    Args:
        user_id: 用户 ID
        limit: 返回的会话数量限制，默认 10

    Returns:
        dict: 包含会话列表的字典

    Example:
        >>> GET /llm/chat/sessions?user_id=user1&limit=10
    """
    try:
        sessions = await chat_service.get_user_sessions(user_id, limit)
        return {"sessions": [session.dict() for session in sessions]}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 