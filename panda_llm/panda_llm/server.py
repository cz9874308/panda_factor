"""
Panda LLM API 服务器模块

本模块是 Panda LLM 服务的入口，创建并配置 FastAPI 应用，注册路由和中间件。
它提供了基于大语言模型的因子开发助手 API 服务。

核心概念
--------

- **FastAPI 应用**：使用 FastAPI 框架构建 Web API
- **CORS 中间件**：配置跨域资源共享，允许前端访问
- **路由注册**：注册聊天相关的 API 路由

为什么需要这个模块？
-------------------

在 Web 应用中，需要提供 API 接口：
- 前端需要调用 LLM 服务进行对话
- 需要支持跨域访问
- 需要统一的 API 入口

这个模块提供了 FastAPI 应用的创建和配置。

工作原理（简单理解）
------------------

就像 Web 服务的入口：

1. **创建应用**：创建 FastAPI 应用实例（就像创建服务器）
2. **配置中间件**：配置 CORS 等中间件（就像配置服务器设置）
3. **注册路由**：注册聊天相关的路由（就像注册服务端点）
4. **启动服务**：启动 API 服务（就像启动服务器）

注意事项
--------

- CORS 配置允许所有来源访问（生产环境建议限制）
- 路由前缀为 /llm，所有聊天接口都在此路径下
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from panda_common.logger_config import logger


# 创建 FastAPI 应用
# 这个应用是整个 LLM 服务的核心，提供 Web API 接口
app = FastAPI(
    title="Panda LLM API",
    description="基于 FastAPI 和 DeepSeek 的聊天 API 服务",
    version="1.0.0"
)

# 配置 CORS（跨域资源共享）
# 允许前端从任何来源访问 API（生产环境建议限制特定来源）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源（生产环境建议限制）
    allow_credentials=True,  # 允许携带凭证
    allow_methods=["*"],  # 允许所有 HTTP 方法
    allow_headers=["*"],  # 允许所有请求头
)

# 导入路由
from panda_llm.routes import chat_router

# 注册路由
# 所有聊天相关的接口都在 /llm 路径下
app.include_router(chat_router.router, prefix="/llm", tags=["chat"])

# 启动日志
logger.info("Panda LLM API 服务已启动") 