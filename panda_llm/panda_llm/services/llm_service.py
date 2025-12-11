"""
大语言模型服务模块

本模块提供了与大语言模型（LLM）交互的服务，使用 OpenAI 兼容的 API（如 DeepSeek）。
它专门针对因子开发场景，配置了系统提示词，限制模型只能回答因子开发相关的问题。

核心概念
--------

- **LLM 服务**：封装与大语言模型的交互逻辑
- **系统提示词**：定义 AI 助手的角色和行为规范
- **流式响应**：支持流式返回，提供更好的用户体验

为什么需要这个模块？
-------------------

在因子开发过程中，需要智能助手帮助：
- 编写和优化因子代码
- 解释内置函数的使用方法
- 调试因子代码
- 提供因子开发建议

这个模块封装了与 LLM 的交互，提供了统一的接口。

工作原理（简单理解）
------------------

就像与 AI 助手对话：

1. **初始化连接**：创建 LLM 客户端连接（就像连接 AI 助手）
2. **配置角色**：设置系统提示词，定义助手角色（就像告诉助手它的职责）
3. **发送消息**：将用户消息和历史对话发送给 LLM（就像提问）
4. **接收回答**：接收 LLM 生成的回答（就像收到回答）

注意事项
--------

- 使用 OpenAI 兼容的 API，支持 DeepSeek 等模型
- 系统提示词限制模型只能回答因子开发相关问题
- 所有回答都使用中文
- 支持流式和非流式两种响应模式
"""

import openai
from panda_common.logger_config import logger
from panda_common.config import get_config
import traceback
import json
from typing import Optional, Dict, List, Any, Union

class LLMService:
    """大语言模型服务

    这个类封装了与大语言模型的交互逻辑，提供了聊天完成和流式响应功能。
    它专门针对因子开发场景，配置了系统提示词，限制模型只能回答因子开发相关的问题。

    为什么需要这个类？
    -----------------

    在因子开发过程中，需要智能助手帮助：
    - 编写和优化因子代码
    - 解释内置函数的使用方法
    - 调试因子代码
    - 提供因子开发建议

    这个类提供了与 LLM 交互的统一接口。

    工作原理（简单理解）
    ------------------

    就像与 AI 助手对话：

    1. **初始化连接**：创建 LLM 客户端连接（就像连接 AI 助手）
    2. **配置角色**：设置系统提示词，定义助手角色（就像告诉助手它的职责）
    3. **发送消息**：将用户消息和历史对话发送给 LLM（就像提问）
    4. **接收回答**：接收 LLM 生成的回答（就像收到回答）

    实际使用场景
    -----------

    发送聊天消息并获取回答：

    ```python
    llm = LLMService()
    messages = [Message(role="user", content="如何计算动量因子？")]
    response = await llm.chat_completion(messages)
    ```

    注意事项
    --------

    - 使用 OpenAI 兼容的 API，支持 DeepSeek 等模型
    - 系统提示词限制模型只能回答因子开发相关问题
    - 所有回答都使用中文
    - 支持流式和非流式两种响应模式
    """
    def __init__(self):
        """初始化 LLM 服务

        这个函数就像"启动 AI 助手"，它会：
        - 从配置中读取 API 密钥、模型和基础 URL
        - 创建 OpenAI 客户端连接
        - 配置系统提示词，定义助手角色

        为什么需要系统提示词？
        --------------------

        系统提示词定义了 AI 助手的角色和行为：
        - 限制助手只能回答因子开发相关问题
        - 确保所有回答都使用中文
        - 提供因子开发的知识和示例

        工作原理
        --------

        1. **读取配置**：从配置中读取 LLM 相关配置
        2. **创建客户端**：创建 OpenAI 兼容的客户端
        3. **配置提示词**：设置系统提示词，定义助手角色

        Raises:
            Exception: 如果配置缺失或客户端创建失败，会抛出异常

        Example:
            >>> llm = LLMService()
        """
        # 从配置中读取 LLM 相关配置
        config = get_config()
        self.api_key = config.get("LLM_API_KEY")  # API 密钥
        self.model = config.get("LLM_MODEL")  # 模型名称
        self.base_url = config.get("LLM_BASE_URL")  # API 基础 URL
        
        # 创建 OpenAI 兼容的客户端（支持 DeepSeek 等模型）
        self.client = openai.OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        
        # 定义系统提示词，限制模型只能作为因子开发助手
        # 这个提示词定义了助手的角色、能力和行为规范
        self.system_message = {
            "role": "system",
            "content": """You are PandaAI Factor Development Assistant, a specialized AI designed to help with quantitative factor development and optimization.

I will ONLY answer questions related to factor development, coding, and optimization. If asked about unrelated topics, I will politely remind users that I'm specialized in factor development.

I WILL ALWAYS RESPOND IN CHINESE regardless of the input language.

I can assist with:
- Writing and optimizing factor code in both formula and Python modes
- Explaining built-in functions for factor development
- Providing examples of factor implementations
- Debugging factor code
- Suggesting improvements to factor logic

My knowledge includes these factor types and functions:

1. Basic Factors:
   - Price factors: CLOSE, OPEN, HIGH, LOW
   - Volume factors: VOLUME, AMOUNT, TURNOVER
   - Market cap factors: MARKET_CAP

2. Factor Development Methods:
   - Formula Mode: Mathematical expressions with built-in functions
   - Python Mode: Custom factor classes implementing the calculate method

3. Built-in Function Libraries with Parameters:
   - Basic calculation:
     * RANK(series) - Cross-sectional ranking, normalized to [-0.5, 0.5]
     * RETURNS(close, period=1) - Calculate returns
     * STDDEV(series, window=20) - Calculate rolling standard deviation
     * CORRELATION(series1, series2, window=20) - Calculate rolling correlation
     * IF(condition, true_value, false_value) - Conditional selection
     * MIN(series1, series2) - Take minimum values
     * MAX(series1, series2) - Take maximum values
     * ABS(series) - Calculate absolute values
     * LOG(series) - Calculate natural logarithm
     * POWER(series, power) - Calculate power

   - Time series:
     * DELAY(series, period=1) - Series delay, returns value from N periods ago
     * SUM(series, window=20) - Calculate moving sum
     * TS_MEAN(series, window=20) - Calculate moving average
     * TS_MIN(series, window=20) - Calculate moving minimum
     * TS_MAX(series, window=20) - Calculate moving maximum
     * TS_RANK(series, window=20) - Calculate time series ranking
     * MA(series, window) - Simple moving average
     * EMA(series, window) - Exponential moving average
     * SMA(series, window, M=1) - Smoothed moving average
     * WMA(series, window) - Weighted moving average

   - Technical indicators:
     * MACD(close, SHORT=12, LONG=26, M=9) - Calculate MACD
     * KDJ(close, high, low, N=9, M1=3, M2=3) - Calculate KDJ
     * RSI(close, N=24) - Calculate Relative Strength Index
     * BOLL(close, N=20, P=2) - Calculate Bollinger Bands
     * CCI(close, high, low, N=14) - Calculate Commodity Channel Index
     * ATR(close, high, low, N=20) - Calculate Average True Range

   - Core utilities:
     * RD(S, D=3) - Round to D decimal places
     * REF(S, N=1) - Shift entire series down by N
     * DIFF(S, N=1) - Calculate difference between values
     * CROSS(S1, S2) - Check for upward cross
     * FILTER(S, N) - Filter signals, only keep first signal in N periods

4. Examples:

   - Formula Mode Examples:
     * Simple momentum: "RANK((CLOSE / DELAY(CLOSE, 20)) - 1)"
     * Volume-price correlation: "CORRELATION(CLOSE, VOLUME, 20)"
     * Complex example: "RANK((CLOSE / DELAY(CLOSE, 20)) - 1) * STDDEV((CLOSE / DELAY(CLOSE, 1)) - 1, 20) * IF(CLOSE > DELAY(CLOSE, 1), 1, -1)"

   - Python Mode Examples:
     * Basic momentum factor:
```python
class MomentumFactor(Factor):
    def calculate(self, factors):
        close = factors['close']
        # Calculate 20-day returns
        returns = (close / DELAY(close, 20)) - 1
        return RANK(returns)
```

     * Complex multi-signal factor:
```python
class ComplexFactor(Factor):
    def calculate(self, factors):
        close = factors['close']
        volume = factors['volume']
        
        # Calculate returns
        returns = (close / DELAY(close, 20)) - 1
        # Calculate volatility
        volatility = STDDEV((close / DELAY(close, 1)) - 1, 20)
        # Calculate volume ratio
        volume_ratio = volume / DELAY(volume, 1)
        # Calculate momentum signal
        momentum = RANK(returns)
        # Calculate volatility signal
        vol_signal = IF(volatility > DELAY(volatility, 1), 1, -1)
        # Combine signals
        result = momentum * vol_signal * (volume_ratio / SUM(volume_ratio, 10))
        return result
```

IMPORTANT: I will not reference functions that don't exist in the system. I will avoid using future data, as the competition rules require out-of-sample running, calculating factor values daily, and placing orders the next day to calculate returns.

For all questions unrelated to factor development, I will politely remind users that I can only help with factor development topics."""
        }

    def _prepare_messages(self, messages):
        """转换消息格式以适配 OpenAI API

        这个函数将内部消息格式转换为 OpenAI API 需要的格式。
        它会添加系统提示词，并转换消息对象为字典格式。

        为什么需要这个函数？
        --------------------

        OpenAI API 需要特定格式的消息：
        - 需要包含系统提示词
        - 消息需要是字典格式，包含 role 和 content

        这个函数提供了格式转换的功能。

        Args:
            messages: 消息列表，可以是 Message 对象或字典格式

        Returns:
            list: 格式化后的消息列表，包含系统提示词和用户消息

        Example:
            >>> messages = [Message(role="user", content="你好")]
            >>> formatted = llm._prepare_messages(messages)
        """
        formatted_messages = []
        
        # 添加系统提示词
        formatted_messages.append(self.system_message)
        
        # 添加用户消息
        for msg in messages:
            if hasattr(msg, 'role') and hasattr(msg, 'content'):
                # 处理 Message 对象
                formatted_messages.append({
                    "role": msg.role,
                    "content": msg.content
                })
            elif isinstance(msg, dict) and 'role' in msg and 'content' in msg:
                # 处理已经是字典格式的消息
                formatted_messages.append({
                    "role": msg["role"],
                    "content": msg["content"]
                })
        
        return formatted_messages

    async def chat_completion(self, messages) -> str:
        """发送聊天请求到 OpenAI API（非流式）

        这个函数发送聊天请求到 LLM，并等待完整回答返回。
        适用于不需要实时响应的场景。

        为什么需要这个函数？
        --------------------

        有些场景不需要流式响应：
        - 简单的问答场景
        - 不需要实时显示回答
        - 需要等待完整回答后再处理

        这个函数提供了非流式的聊天完成功能。

        Args:
            messages: 消息列表，包含对话历史

        Returns:
            str: LLM 生成的完整回答

        Raises:
            Exception: 如果 API 调用失败，会抛出异常

        Example:
            >>> messages = [Message(role="user", content="如何计算动量因子？")]
            >>> response = await llm.chat_completion(messages)
        """
        try:
            # 格式化消息
            formatted_messages = self._prepare_messages(messages)
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=formatted_messages,
                temperature=0.7,
                max_tokens=2000,
                stream=False
            )
            content = response.choices[0].message.content
            return content if content is not None else ""
        except Exception as e:
            traceback.print_exc()
            logger.error(f"调用 OpenAI API 失败: {str(e)}")
            raise

    async def chat_completion_stream(self, messages):
        """发送流式聊天请求到 OpenAI API

        这个函数发送聊天请求到 LLM，并以流式方式返回回答。
        适用于需要实时显示回答的场景，提供更好的用户体验。

        为什么需要这个函数？
        --------------------

        流式响应提供更好的用户体验：
        - 用户可以实时看到回答生成过程
        - 不需要等待完整回答，响应更快
        - 提供类似 ChatGPT 的交互体验

        这个函数提供了流式的聊天完成功能。

        Args:
            messages: 消息列表，包含对话历史

        Yields:
            str: LLM 生成的回答片段（逐个 token）

        Raises:
            Exception: 如果 API 调用失败，会抛出异常

        Example:
            >>> messages = [Message(role="user", content="如何计算动量因子？")]
            >>> async for chunk in llm.chat_completion_stream(messages):
            ...     print(chunk, end='')
        """
        try:
            # 格式化消息
            formatted_messages = self._prepare_messages(messages)
            
            stream = self.client.chat.completions.create(
                model=self.model,
                messages=formatted_messages,
                temperature=0.1,
                max_tokens=2000,
                stream=True
            )
            
            for chunk in stream:
                content = chunk.choices[0].delta.content
                if content:
                    yield content
        except Exception as e:
            traceback.print_exc()
            logger.error(f"调用 OpenAI API 流式请求失败: {str(e)}")
            raise 