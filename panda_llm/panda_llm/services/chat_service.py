"""
聊天服务模块

本模块提供了聊天服务的核心业务逻辑，包括消息处理、会话管理和流式响应。
它协调 MongoDB 服务和 LLM 服务，提供完整的聊天功能。

核心概念
--------

- **消息处理**：处理用户消息，调用 LLM 生成回答
- **会话管理**：管理聊天会话的创建、查询和删除
- **流式响应**：支持流式返回 AI 回答，提供更好的用户体验

为什么需要这个模块？
-------------------

在聊天服务中，需要协调多个服务：
- 需要管理聊天会话
- 需要调用 LLM 生成回答
- 需要保存对话历史
- 需要支持流式响应

这个模块提供了完整的聊天服务能力。

工作原理（简单理解）
------------------

就像聊天系统的核心：

1. **接收消息**：接收用户的消息（就像接收消息）
2. **管理会话**：获取或创建聊天会话（就像管理对话）
3. **调用 AI**：调用 LLM 生成回答（就像让 AI 回答）
4. **保存历史**：保存对话历史到数据库（就像保存记录）
5. **返回回答**：返回 AI 的回答（就像返回回答）

注意事项
--------

- 支持流式和非流式两种响应模式
- 自动管理会话的创建和更新
- 所有对话历史都会保存到数据库
"""

from datetime import datetime
from typing import List, Optional, AsyncGenerator
from panda_common.logger_config import logger
from panda_llm.services.mongodb import MongoDBService
from panda_llm.models.chat import ChatSession, Message
from panda_llm.services.llm_service import LLMService
import uuid


class ChatService:
    """聊天服务

    这个类提供了聊天服务的核心业务逻辑，包括消息处理、会话管理和流式响应。
    它协调 MongoDB 服务和 LLM 服务，提供完整的聊天功能。

    为什么需要这个类？
    -----------------

    在聊天服务中，需要协调多个服务：
    - 需要管理聊天会话
    - 需要调用 LLM 生成回答
    - 需要保存对话历史
    - 需要支持流式响应

    这个类提供了完整的聊天服务能力。

    实际使用场景
    -----------

    处理用户消息并获取回答：

    ```python
    service = ChatService()
    response = await service.process_message("session123", "如何计算动量因子？", "user1")
    ```

    注意事项
    --------

    - 支持流式和非流式两种响应模式
    - 自动管理会话的创建和更新
    - 所有对话历史都会保存到数据库
    """
    def __init__(self):
        self.mongodb = MongoDBService()
        self.logger = logger

    async def process_message(self, session_id: str, user_message: str, user_id: str) -> str:
        """处理用户消息并返回 AI 响应"""
        try:
            # 获取或创建会话
            session = await self.mongodb.get_chat_session(session_id)
            if not session:
                # 创建新会话时生成唯一 ID
                session = ChatSession(
                    id=str(uuid.uuid4()),
                    user_id=user_id,
                    messages=[],
                    created_at=datetime.now().isoformat(),
                    updated_at=datetime.now().isoformat()
                )
                await self.mongodb.create_chat_session(session)

            # 添加用户消息
            user_msg = Message(role="user", content=user_message)
            session.messages.append(user_msg)
            await self.mongodb.update_chat_session(session.id, session)

            # 调用 AI 服务
            llm = LLMService()
            ai_response = await llm.chat_completion(session.messages)

            # 添加 AI 响应
            ai_msg = Message(role="assistant", content=ai_response)
            session.messages.append(ai_msg)
            await self.mongodb.update_chat_session(session.id, session)

            return ai_response

        except Exception as e:
            self.logger.error(f"处理消息失败: {str(e)}")
            raise

    async def get_session_messages(self, session_id: str) -> List[Message]:
        """获取会话消息历史"""
        session = await self.mongodb.get_chat_session(session_id)
        return session.messages if session else []

    async def clear_session(self, session_id: str):
        """清空会话"""
        await self.mongodb.delete_chat_session(session_id)

    async def process_message_stream(self, user_id: str, message: str, session_id: Optional[str] = None) -> AsyncGenerator[str, None]:
        """处理用户消息并流式返回回复

        这个函数处理用户消息，并以流式方式返回 AI 的回答。
        适用于需要实时显示回答的场景，提供更好的用户体验。

        为什么需要这个函数？
        --------------------

        流式响应提供更好的用户体验：
        - 用户可以实时看到回答生成过程
        - 不需要等待完整回答，响应更快
        - 提供类似 ChatGPT 的交互体验

        工作原理
        --------

        1. 创建用户消息
        2. 获取或创建会话
        3. 保存用户消息到会话
        4. 调用 LLM 流式生成回答
        5. 逐个返回回答片段
        6. 保存完整回答到会话

        Args:
            user_id: 用户 ID
            message: 用户消息
            session_id: 会话 ID（可选，如果不存在则创建新会话）

        Yields:
            str: AI 回答的片段（逐个 token）

        Raises:
            ValueError: 如果指定的会话不存在
            Exception: 如果处理消息失败

        Example:
            >>> service = ChatService()
            >>> async for chunk in service.process_message_stream("user1", "你好"):
            ...     print(chunk, end='')
        """
        try:
            # 创建用户消息
            user_message = Message(
                role="user",
                content=message,
                timestamp=datetime.now().isoformat()
            )

            # 获取或创建会话
            if session_id:
                session = await self.mongodb.get_chat_session(session_id)
                if not session:
                    logger.error(f"会话不存在: {session_id}")
                    raise ValueError(f"会话不存在: {session_id}")
            else:
                session = ChatSession(
                    id=str(uuid.uuid4()),
                    user_id=user_id,
                    messages=[user_message],
                    created_at=datetime.now().isoformat(),
                    updated_at=datetime.now().isoformat()
                )
                session_id = await self.mongodb.create_chat_session(session)
                logger.info(f"创建新会话: {session_id}")

            # 更新会话
            session.messages.append(user_message)
            await self.mongodb.update_chat_session(session.id, session)

            # 准备历史消息
            messages = [{"role": msg.role, "content": msg.content} for msg in session.messages]

            # 调用 AI 服务
            llm = LLMService()
            full_response = ""
            async for chunk in llm.chat_completion_stream(messages):
                full_response += chunk
                yield chunk

            # 添加 AI 响应
            ai_msg = Message(role="assistant", content=full_response)
            session.messages.append(ai_msg)
            await self.mongodb.update_chat_session(session.id, session)

        except Exception as e:
            self.logger.error(f"处理消息失败: {str(e)}")
            raise

    async def get_user_sessions(self, user_id: str, limit: int = 10) -> List[ChatSession]:
        """获取用户的聊天会话列表"""
        try:
            sessions = await self.mongodb.get_user_sessions(user_id)
            return sessions[:limit]
        except Exception as e:
            self.logger.error(f"获取用户会话列表失败: {str(e)}")
            raise 