"""
图表数据模型模块

本模块定义了用于图表展示的数据模型，就像一个"图表数据模板库"，
它提供了标准化的数据结构，确保前端图表能够正确显示因子分析的结果。

核心概念
--------

- **SeriesItem**：单个系列数据项，代表图表中的一条数据线
- **ChartData**：图表数据基类，包含图表的标题和坐标轴数据

使用方式
--------

1. 创建 `SeriesItem` 实例，定义数据系列
2. 创建 `ChartData` 实例，组装完整的图表数据
3. 将数据序列化为 JSON，传递给前端展示

工作原理
--------

就像制作图表的过程：

1. **准备数据系列**：创建 `SeriesItem` 实例（就像准备图表中的一条线）
2. **组装图表**：创建 `ChartData` 实例，包含标题和坐标轴数据（就像组装完整的图表）
3. **序列化输出**：将数据转换为 JSON 格式（就像将图表转换为可传输的格式）
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Union, Any
from datetime import datetime


class SeriesItem(BaseModel):
    """单个系列数据项：图表中的一条数据线

    这个类就像一个"数据线模板"，它定义了图表中一条数据线的基本结构，
    包括数据线的名称和数据点列表。

    为什么需要这个类？
    --------------------

    在图表中，通常需要显示多条数据线，比如：
    - 不同分组的收益率曲线
    - 不同滞后期数的 IC 值
    - 基准和策略的对比曲线

    这个类提供了标准化的数据结构，让前端可以统一处理各种数据系列。

    工作原理
    --------

    就像画图表时的一条线：

    1. **命名**：给数据线起个名字（就像给线条贴标签）
    2. **数据点**：提供一系列数据点（就像在坐标轴上标记点）
    3. **连接**：前端会根据这些点画出线条（就像用线连接这些点）

    Attributes:
        name: 系列名称，如 "第1组"、"基准"、"策略" 等
        data: 数据点列表，可以是浮点数（数值）或字符串（标签）

    Example:
        >>> series = SeriesItem(
        ...     name="第1组",
        ...     data=[0.1, 0.2, 0.15, 0.3]
        ... )
    """
    name: str = Field(..., description="系列名称")
    data: List[Union[float, str]] = Field(..., description="系列数据点")


class ChartData(BaseModel):
    """图表数据基类：完整的图表数据结构

    这个类就像一个"图表模板"，它定义了完整图表的数据结构，
    包括图表标题和 X、Y 轴的数据系列。

    为什么需要这个类？
    --------------------

    在因子分析中，需要展示各种图表，比如：
    - 收益率曲线图
    - IC 时序图
    - IC 衰减图
    - 分组收益对比图

    这个类提供了统一的图表数据结构，让前端可以统一处理和展示。

    工作原理
    --------

    就像制作一个完整的图表：

    1. **标题**：设置图表的标题（就像给图表起个名字）
    2. **X 轴**：定义 X 轴的数据系列（就像设置横坐标）
    3. **Y 轴**：定义 Y 轴的数据系列（就像设置纵坐标）
    4. **展示**：前端根据这些数据绘制图表（就像根据模板画出图表）

    Attributes:
        title: 图表标题，如 "收益率曲线"、"IC 时序图" 等
        x: X 轴数据系列列表，每个元素是一个 `SeriesItem`
        y: Y 轴数据系列列表，每个元素是一个 `SeriesItem`

    Example:
        >>> chart = ChartData(
        ...     title="收益率曲线",
        ...     x=[SeriesItem(name="日期", data=["2024-01-01", "2024-01-02"])],
        ...     y=[SeriesItem(name="收益率", data=[0.1, 0.2])]
        ... )
    """
    title: str = Field(..., description="图表标题")
    x: List[SeriesItem] = Field(..., description="x轴数据")
    y: List[SeriesItem] = Field(..., description="y轴数据")


# class ReturnChartData(BaseModel):
#     """收益率图表数据"""
#     series: List[SeriesItem] = Field(..., description="收益率系列数据")
#     dates: List[str] = Field(..., description="日期列表")


# class ICTimeData(BaseModel):
#     """IC时序图数据"""
#     title: str = Field(..., description="图表标题")
#     x_label: str = Field(..., description="x轴标签")
#     x_data: List[str] = Field(..., description="x轴数据")
#     y: Dict[str, Any] = Field(..., description="y轴数据，包含多个系列")


# class ICHistData(BaseModel):
#     """IC直方图数据"""
#     histogram: Dict[str, List[float]] = Field(..., description="直方图数据")
#     normal_curve: Dict[str, List[float]] = Field(..., description="正态分布曲线")
#     stats: Dict[str, float] = Field(..., description="统计指标")


# class ICDecayData(BaseModel):
#     """IC衰减图数据"""
#     lag: int = Field(..., description="滞后期数")
#     value: float = Field(..., description="IC值")


# class ACFData(BaseModel):
#     """自相关图数据"""
#     lag: int = Field(..., description="滞后期数")
#     acf: float = Field(..., description="自相关系数")
#     lower_bound: float = Field(..., description="下界")
#     upper_bound: float = Field(..., description="上界")


# class FactorStat(BaseModel):
#     """因子统计指标"""
#     group_name: str = Field(..., description="分组名称")
#     年化收益率: Optional[str] = Field(None, description="年化收益率")
#     超额年化: Optional[str] = Field(None, description="超额年化")
#     最大回撤: Optional[str] = Field(None, description="最大回撤")
#     超额最大回撤: Optional[str] = Field(None, description="超额最大回撤")
#     年化波动: Optional[str] = Field(None, description="年化波动")
#     超额年化波动: Optional[str] = Field(None, description="超额年化波动")
#     换手率: Optional[str] = Field(None, description="换手率")
#     月度胜率: Optional[str] = Field(None, description="月度胜率")
#     超额月度胜率: Optional[str] = Field(None, description="超额月度胜率")
#     跟踪误差: Optional[str] = Field(None, description="跟踪误差")
#     夏普比率: Optional[str] = Field(None, description="夏普比率")
#     信息比率: Optional[str] = Field(None, description="信息比率")


# class ICStat(BaseModel):
#     """IC统计指标"""
#     metric: str = Field(..., description="指标名称")
#     value: str = Field(..., description="指标值")


# class TopFactor(BaseModel):
#     """Top因子数据"""
#     symbol: str = Field(..., description="股票代码")
#     name: Optional[str] = Field("", description="股票名称")
#     value: float = Field(..., description="因子值")


# class FactorAnalysisChartData(BaseModel):
#     """完整因子分析图表数据"""
#     return_chart: ReturnChartData
#     excess_return_chart: ReturnChartData
#     ic_time_chart: Optional[ICTimeData] = None
#     ic_hist_chart: Optional[ICHistData] = None
#     ic_decay_data: Optional[List[ICDecayData]] = Field(default_factory=list)
#     acf_data: Optional[List[ACFData]] = Field(default_factory=list)
#     group_stats: Optional[List[FactorStat]] = Field(default_factory=list)
#     ic_stats: Optional[List[ICStat]] = Field(default_factory=list)
#     top_factors: Optional[List[TopFactor]] = Field(default_factory=list)
    
#     class Config:
#         json_encoders = {
#             datetime: lambda v: v.strftime("%Y-%m-%d %H:%M:%S")
#         }
        
#     def dict(self, *args, **kwargs):
#         """转换为字典，处理None值"""
#         result = super().dict(*args, **kwargs)
#         # 处理可能的None值，确保JSON序列化不会出错
#         return {k: ([] if v is None and isinstance(v, list) else v) for k, v in result.items()}

