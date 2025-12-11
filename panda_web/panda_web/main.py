"""
Panda Web 前端服务模块

本模块是 Panda Web 前端应用的入口，使用 FastAPI 提供静态文件服务。
它主要用于托管前端静态资源（HTML、CSS、JavaScript 等）。

核心概念
--------

- **静态文件服务**：提供前端静态资源的访问
- **FastAPI 应用**：使用 FastAPI 构建 Web 服务器
- **前端应用**：Vue.js 等前端框架构建的单页应用

为什么需要这个模块？
-------------------

在 Web 应用中，需要提供前端页面：
- 用户需要通过浏览器访问前端界面
- 前端需要访问静态资源
- 需要提供统一的入口

这个模块提供了前端静态文件服务。

工作原理（简单理解）
------------------

就像 Web 服务器：

1. **启动服务**：启动 FastAPI 应用（就像启动 Web 服务器）
2. **托管静态文件**：提供静态文件的访问（就像提供文件下载）
3. **处理请求**：处理前端的资源请求（就像处理 HTTP 请求）

注意事项
--------

- 主要用于托管前端静态资源
- 前端通过 API 与后端服务通信
- 静态文件通常由前端构建工具生成
"""

import os
import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

# 创建 FastAPI 应用，用于托管前端静态文件
app = FastAPI(title="PandaAI Web Interface")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Get the absolute path to the static directory
DIST_DIR = os.path.join(os.path.dirname(__file__), "static")

# Mount the Vue dist directory at /factor path
app.mount("/factor", StaticFiles(directory=DIST_DIR, html=True), name="static")

# Redirect root to /factor
@app.get("/")
async def redirect_to_factor():
    return RedirectResponse(url="/factor")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8080,
        reload=True  # Enable auto-reload during development
    ) 