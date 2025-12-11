"""
因子分析参数模型模块

本模块定义了因子分析所需的参数模型，就像一个"分析配置模板"，
它提供了标准化的参数结构，确保因子分析过程能够正确配置和执行。

核心概念
--------

- **Params**：因子分析参数类，包含所有分析所需的配置参数
- **参数验证**：使用 Pydantic 的验证器确保参数格式正确

使用方式
--------

1. 创建 `Params` 实例，设置分析参数
2. 传递给因子分析函数
3. 系统会根据参数执行分析

工作原理
--------

就像设置实验参数：

1. **设置时间范围**：指定分析的开始和结束日期（就像设置实验的时间段）
2. **设置分析规则**：指定调仓周期、股票池、分组数量等（就像设置实验规则）
3. **执行分析**：系统根据参数执行分析（就像按照规则进行实验）
"""

# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field, validator
from datetime import date


class Params(BaseModel):
    """因子分析参数类：配置因子分析的所有参数

    这个类就像一个"分析配置单"，它包含了因子分析所需的所有参数，
    让你可以灵活配置分析的时间范围、股票池、分组方式等。

    为什么需要这个类？
    --------------------

    在因子分析中，需要配置很多参数：
    - 分析的时间范围
    - 调仓周期（多久调整一次持仓）
    - 股票池（分析哪些股票）
    - 分组数量（将股票分成几组）
    - 因子方向（因子值越大越好还是越小越好）

    如果这些参数分散传递，会导致：
    - 参数管理混乱
    - 参数验证困难
    - 代码可读性差

    这个类将所有参数集中管理，解决了这些问题。

    工作原理（简单理解）
    ------------------

    就像设置一个实验：

    1. **时间范围**：设置实验的开始和结束时间（就像设置实验的时间段）
    2. **实验规则**：设置调仓周期、股票池、分组数量等（就像设置实验规则）
    3. **数据处理**：设置极值处理方式（就像设置数据清洗规则）
    4. **执行分析**：系统根据这些参数执行分析（就像按照规则进行实验）

    实际使用场景
    -----------

    在创建因子分析任务时使用：

    ```python
    params = Params(
        start_date="2023-01-01",
        end_date="2024-01-01",
        adjustment_cycle=5,
        stock_pool="000300",
        factor_direction=True,
        group_number=10,
        include_st=False,
        extreme_value_processing="中位数"
    )
    ```

    参数说明
    --------

    - **start_date/end_date**：分析的时间范围，格式为 "YYYY-MM-DD"
    - **adjustment_cycle**：调仓周期，单位是天，如 1 表示每天调仓，5 表示每 5 天调仓
    - **stock_pool**：股票池代码，如 "000300"（沪深300）、"000905"（中证500）、"000985"（全A股）
    - **factor_direction**：因子方向，True 表示因子值越大越好，False 表示因子值越小越好
    - **group_number**：分组数量，范围 2-20，将股票分成几组进行分析
    - **include_st**：是否包含 ST 股票，True 表示包含，False 表示不包含
    - **extreme_value_processing**：极值处理方式，如 "中位数"、"标准差" 等

    Attributes:
        start_date: 开始时间，格式 "YYYY-MM-DD"
        end_date: 结束时间，格式 "YYYY-MM-DD"
        adjustment_cycle: 调仓周期，单位天
        stock_pool: 股票池代码
        factor_direction: 因子方向，True 正向，False 负向
        group_number: 分组数量，范围 2-20
        include_st: 是否包含 ST 股票
        extreme_value_processing: 极值处理方式
    """
    # 回测区间：1年、3年、5年
    start_date: str = Field(..., example="2023-01-01", description="开始时间")
    end_date: str = Field(..., example="2024-01-01", description="结束时间")

    # 调仓周期：N天
    adjustment_cycle: int = Field(..., example=1, description="调仓周期，单位天")

    # 股票池：沪深300、中证500、全A股
    stock_pool: str = Field(..., example="沪深300", description="股票池")

    # 因子方向：正向、负向
    factor_direction: bool = Field(..., example=False, description="因子方向：False 负向，True 正向")

    # 分组数量：2-20
    group_number: int = Field(..., example=2, ge=2, le=20, description="分组数量：2-20")

    # 是否包含ST
    include_st: bool = Field(...,example=False, description="是否包含ST：False 不包含，True 包含")

    # 极值处理：标准差、中位数
    extreme_value_processing: str = Field(..., example="中位数", description="极值处理方式")

    @validator('start_date', 'end_date')
    def validate_dates(cls, v):
        """验证日期格式

        这个验证器确保日期字符串的格式正确，必须是 "YYYY-MM-DD" 格式。

        为什么需要这个验证器？
        --------------------

        日期格式错误会导致分析失败，在参数设置时就验证可以：
        - 提前发现问题
        - 提供清晰的错误提示
        - 避免在分析过程中才发现错误

        工作原理
        --------

        1. 尝试将字符串解析为日期对象
        2. 如果成功，转换为标准格式返回
        3. 如果失败，抛出清晰的错误信息

        Args:
            v: 日期字符串

        Returns:
            str: 标准格式的日期字符串 "YYYY-MM-DD"

        Raises:
            ValueError: 如果日期格式不正确

        Example:
            >>> params = Params(...)
            >>> # 如果 start_date="2023/01/01"，会抛出 ValueError
            >>> # 如果 start_date="2023-01-01"，会通过验证
        """
        try:
            return date.fromisoformat(v).isoformat()
        except ValueError:
            raise ValueError('Invalid date format. Use YYYY-MM-DD')