"""
MongoDB 数据服务模块

本模块提供了聊天会话的 MongoDB 数据访问服务，包括会话的创建、查询、更新和删除。
它封装了与 MongoDB 的交互逻辑，提供了简洁的接口。

核心概念
--------

- **聊天会话**：存储用户的聊天对话历史
- **MongoDB 操作**：封装了 CRUD 操作
- **会话管理**：支持会话的创建、查询、更新和删除

为什么需要这个模块？
-------------------

在聊天服务中，需要持久化存储对话历史：
- 用户需要查看历史对话
- 需要支持多轮对话
- 需要管理用户的多个会话

这个模块提供了 MongoDB 数据访问的能力。

工作原理（简单理解）
------------------

就像数据库访问层：

1. **连接数据库**：通过 DatabaseHandler 连接 MongoDB（就像连接数据库）
2. **执行操作**：执行 CRUD 操作（就像执行 SQL）
3. **返回结果**：返回操作结果（就像返回查询结果）

注意事项
--------

- 支持 ObjectId 和字符串两种 session_id 格式
- 所有操作都是异步的
- 错误会记录日志并抛出异常
"""

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.config import config
from panda_common.logger_config import logger
from panda_llm.models.chat import *
from bson import ObjectId


class MongoDBService:
    """MongoDB 数据服务

    这个类提供了聊天会话的 MongoDB 数据访问服务，包括会话的创建、查询、更新和删除。
    它封装了与 MongoDB 的交互逻辑，提供了简洁的接口。

    为什么需要这个类？
    -----------------

    在聊天服务中，需要持久化存储对话历史：
    - 用户需要查看历史对话
    - 需要支持多轮对话
    - 需要管理用户的多个会话

    这个类提供了 MongoDB 数据访问的能力。

    实际使用场景
    -----------

    创建新的聊天会话：

    ```python
    service = MongoDBService()
    session = ChatSession(id="123", user_id="user1", messages=[])
    await service.create_chat_session(session)
    ```

    注意事项
    --------

    - 支持 ObjectId 和字符串两种 session_id 格式
    - 所有操作都是异步的
    - 错误会记录日志并抛出异常
    """
    def __init__(self):
        self.db_handler = DatabaseHandler(config)
        self.collection = self.db_handler.get_mongo_collection("panda","chat_sessions")
        self.logger = logger

    async def create_chat_session(self, session: ChatSession) -> str:
        """创建新的聊天会话"""
        try:
            result = self.collection.insert_one(session.dict())
            return str(result.inserted_id)
        except Exception as e:
            self.logger.error(f"创建会话失败: {str(e)}")
            raise

    async def get_chat_session(self, session_id: str) -> Optional[ChatSession]:
        """获取聊天会话"""
        try:
            # 尝试将 session_id 转换为 ObjectId
            try:
                query = {"_id": ObjectId(session_id)}
            except:
                # 如果不是有效的 ObjectId，则使用原始字符串
                query = {"_id": session_id}
                
            session = self.collection.find_one(query)
            if session:
                return ChatSession(**session)
            return None
        except Exception as e:
            self.logger.error(f"获取会话失败: {str(e)}")
            raise

    async def update_chat_session(self, session_id: str, session: ChatSession):
        """更新聊天会话"""
        try:
            # 尝试将 session_id 转换为 ObjectId
            try:
                query = {"_id": ObjectId(session_id)}
            except:
                # 如果不是有效的 ObjectId，则使用原始字符串
                query = {"_id": session_id}
                
            self.collection.update_one(
                query,
                {"$set": session.dict()}
            )
        except Exception as e:
            self.logger.error(f"更新会话失败: {str(e)}")
            raise

    async def delete_chat_session(self, session_id: str):
        """删除聊天会话"""
        try:
            # 尝试将 session_id 转换为 ObjectId
            try:
                query = {"_id": ObjectId(session_id)}
            except:
                # 如果不是有效的 ObjectId，则使用原始字符串
                query = {"_id": session_id}
                
            self.collection.delete_one(query)
        except Exception as e:
            self.logger.error(f"删除会话失败: {str(e)}")
            raise

    async def get_user_sessions(self, user_id: str) -> List[ChatSession]:
        """获取用户的所有会话"""
        try:
            sessions = list(self.collection.find({"user_id": user_id}))
            return [ChatSession(**session) for session in sessions]
        except Exception as e:
            self.logger.error(f"获取用户会话失败: {str(e)}")
            raise