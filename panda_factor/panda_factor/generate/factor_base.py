"""
因子基类模块

本模块定义了所有因子的基类 `Factor`，它提供了因子计算的基础框架和常用工具方法。
就像一个"因子工厂的模板"，所有自定义因子都需要继承这个基类。

核心概念
--------

- **抽象基类**：定义了因子必须实现的接口（calculate 方法）
- **工具方法**：提供了常用的因子计算函数（如 RANK、RETURNS、STDDEV 等）
- **工具类集成**：自动将 FactorUtils 中的静态方法集成到因子实例中

为什么需要这个模块？
-------------------

在量化分析中，因子计算有很多共同的需求：
- 都需要实现计算逻辑
- 都需要使用一些常用的函数（如排名、收益率、标准差等）
- 都需要访问日志功能

如果每个因子都自己实现这些功能，会导致：
- 代码重复
- 接口不统一
- 维护困难

这个基类统一管理这些功能，解决了这些问题。

工作原理（简单理解）
------------------

就像工厂的生产模板：

1. **定义模板**：基类定义了因子必须实现的方法（就像定义生产流程）
2. **提供工具**：基类提供了常用的工具方法（就像提供生产工具）
3. **子类实现**：子类继承基类，实现具体的计算逻辑（就像按照模板生产产品）

注意事项
--------

- 所有自定义因子必须继承这个基类
- 必须实现 `calculate` 方法
- 可以使用基类提供的工具方法简化计算
"""

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
from .factor_utils import FactorUtils


class Factor(ABC):
    """因子基类

    这个类是所有因子的基础模板，它定义了因子计算的标准接口和常用工具方法。
    就像一个"因子工厂的模板"，所有自定义因子都需要继承这个类。

    为什么需要这个类？
    -----------------

    在量化分析中，因子计算有很多共同的需求：
    - 都需要实现计算逻辑
    - 都需要使用一些常用的函数（如排名、收益率、标准差等）
    - 都需要访问日志功能

    如果每个因子都自己实现这些功能，会导致：
    - 代码重复
    - 接口不统一
    - 维护困难

    这个基类统一管理这些功能，解决了这些问题。

    工作原理（简单理解）
    ------------------

    就像工厂的生产模板：

    1. **定义模板**：基类定义了因子必须实现的方法（就像定义生产流程）
    2. **提供工具**：基类提供了常用的工具方法（就像提供生产工具）
    3. **子类实现**：子类继承基类，实现具体的计算逻辑（就像按照模板生产产品）

    实际使用场景
    -----------

    创建自定义因子：

    ```python
    class MyFactor(Factor):
        def calculate(self, factors):
            close = factors['close']
            volume = factors['volume']
            # 使用基类提供的工具方法
            returns = self.RETURNS(close)
            rank = self.RANK(volume)
            return returns * rank
    ```

    注意事项
    --------

    - 所有自定义因子必须继承这个类
    - 必须实现 `calculate` 方法
    - 可以使用基类提供的工具方法简化计算
    """

    def __init__(self):
        """初始化因子实例

        这个函数就像"准备因子计算工具"，它会：
        - 初始化日志记录器（默认为 None）
        - 将 FactorUtils 中的所有公共方法复制到实例中，作为实例方法

        为什么需要复制 FactorUtils 的方法？
        --------------------------------

        FactorUtils 是一个工具类，提供了很多静态方法。
        如果每次使用都要写 `FactorUtils.method_name()`，会很繁琐。

        通过将这些方法复制到实例中，可以直接使用 `self.method_name()`，
        就像这些方法是因子类的一部分，使用起来更方便。

        工作原理
        --------

        1. **初始化日志**：将 logger 设置为 None（后续可以通过 set_factor_logger 设置）
        2. **扫描工具类**：遍历 FactorUtils 类的所有方法
        3. **复制公共方法**：将非私有方法（不以 _ 开头）复制到当前实例
        4. **作为实例方法**：复制后的方法可以直接通过 self 调用

        Example:
            >>> class MyFactor(Factor):
            ...     def calculate(self, factors):
            ...         # 可以直接使用 self.RANK()，而不需要 FactorUtils.RANK()
            ...         return self.RANK(factors['close'])
        """
        self.logger = None  # 日志记录器，用于记录因子计算过程中的日志
        # 将 FactorUtils 中的所有公共方法复制到实例中，作为实例方法
        # 这样可以直接使用 self.method_name()，而不需要 FactorUtils.method_name()
        for method_name in dir(FactorUtils):
            if not method_name.startswith('_'):  # 跳过私有方法（以 _ 开头）
                method = getattr(FactorUtils, method_name)
                setattr(self, method_name, method)

    def set_factor_logger(self, logger):
        """设置因子日志记录器

        这个函数用于设置因子的日志记录器，让因子在计算过程中可以记录日志。

        为什么需要这个函数？
        --------------------

        在因子计算过程中，可能需要记录一些信息：
        - 计算进度
        - 警告信息
        - 错误信息

        通过设置日志记录器，因子可以将这些信息记录到日志中，方便调试和监控。

        工作原理
        --------

        1. 接收日志记录器对象
        2. 保存到实例变量中
        3. 后续可以通过 self.logger 使用

        Args:
            logger: 日志记录器对象，通常是 logging.Logger 实例

        Example:
            >>> import logging
            >>> logger = logging.getLogger("my_factor")
            >>> factor = MyFactor()
            >>> factor.set_factor_logger(logger)
            >>> # 现在可以在 calculate 方法中使用 self.logger.info() 等
        """
        self.logger = logger

    @abstractmethod
    def calculate(self, factors):
        """计算因子值（抽象方法，必须由子类实现）

        这个函数是因子计算的核心，所有自定义因子都必须实现这个方法。
        它接收基础因子数据，计算并返回因子值。

        为什么需要这个抽象方法？
        ------------------------

        不同的因子有不同的计算逻辑，但都需要：
        - 接收基础因子数据（如 open、close、volume）
        - 计算因子值
        - 返回计算结果

        通过定义抽象方法，确保所有因子都实现了这个接口，保证了接口的统一性。

        工作原理（简单理解）
        ------------------

        就像工厂的生产流程：

        1. **接收原材料**：接收基础因子数据（就像接收原材料）
        2. **加工生产**：根据因子的计算逻辑处理数据（就像按照流程生产）
        3. **返回产品**：返回计算好的因子值（就像返回生产好的产品）

        实际使用场景
        -----------

        实现一个简单的动量因子：

        ```python
        class MomentumFactor(Factor):
            def calculate(self, factors):
                close = factors['close']
                # 计算收益率
                returns = self.RETURNS(close)
                # 计算过去20天的收益率总和
                momentum = self.SUM(returns, window=20)
                return momentum
        ```

        实现一个复杂的因子：

        ```python
        class ComplexFactor(Factor):
            def calculate(self, factors):
                close = factors['close']
                volume = factors['volume']
                # 计算收益率
                returns = self.RETURNS(close)
                # 计算成交量的排名
                volume_rank = self.RANK(volume)
                # 计算收益率的标准差
                returns_std = self.STDDEV(returns, window=20)
                # 组合计算
                result = returns * volume_rank / returns_std
                return result
        ```

        注意事项
        --------

        - 输入数据 `factors` 是一个字典，键是因子名称（如 'close'、'volume'），值是 pd.Series
        - 返回的 Series 必须有 (date, symbol) 多级索引
        - 可以使用基类提供的工具方法简化计算

        Args:
            factors: 字典，包含基础因子数据
                     键是因子名称（如 'close'、'volume'、'open' 等）
                     值是 pd.Series，索引为 (date, symbol) 多级索引

        Returns:
            pd.Series: 计算好的因子值，索引为 (date, symbol) 多级索引

        Raises:
            NotImplementedError: 如果子类没有实现这个方法

        Example:
            >>> class MyFactor(Factor):
            ...     def calculate(self, factors):
            ...         close = factors['close']
            ...         return self.RETURNS(close)
        """
        pass

    def RANK(self, series: pd.Series) -> pd.Series:
        """横截面排名，归一化到 [-0.5, 0.5] 范围

        这个函数就像一个"排名系统"，它会将每个日期的所有股票按照数值大小进行排名，
        并将排名归一化到 [-0.5, 0.5] 范围。

        为什么需要这个函数？
        --------------------

        在因子分析中，经常需要对股票进行排名：
        - 动量因子：按照收益率排名
        - 价值因子：按照估值指标排名
        - 质量因子：按照财务指标排名

        排名后的值需要归一化，以便：
        - 不同因子的值可以在同一尺度上比较
        - 方便进行因子组合

        工作原理（简单理解）
        ------------------

        就像班级排名：

        1. **按日期分组**：将数据按日期分组（就像按考试日期分组）
        2. **计算排名**：对每个日期的所有股票进行排名（就像对每次考试的学生排名）
        3. **归一化**：将排名归一化到 [-0.5, 0.5]（就像将排名转换为标准分数）
           - 排名第1的股票：接近 0.5
           - 排名中间的股票：接近 0
           - 排名最后的股票：接近 -0.5

        实际使用场景
        -----------

        计算成交量的排名：

        ```python
        volume_rank = self.RANK(factors['volume'])
        # 返回的 Series 中，每个日期的股票都按照成交量排名，值在 [-0.5, 0.5] 之间
        ```

        Args:
            series: 要排名的 Series，索引为 (date, symbol) 多级索引

        Returns:
            pd.Series: 排名后的 Series，值在 [-0.5, 0.5] 之间，索引为 (date, symbol)

        Example:
            >>> factor = MyFactor()
            >>> volume_rank = factor.RANK(factors['volume'])
            >>> print(volume_rank.head())
        """
        def rank_group(group):
            """对单个日期的数据进行排名"""
            # 去除缺失值
            valid_data = group.dropna()
            if len(valid_data) == 0:
                # 如果没有有效数据，返回全0
                return pd.Series(0, index=group.index)
            # 计算排名（使用平均排名法处理相同值）
            ranks = valid_data.rank(method='average')
            # 归一化到 [-0.5, 0.5] 范围
            # (ranks - 1) / (len - 1) 将排名归一化到 [0, 1]
            # 再减去 0.5 得到 [-0.5, 0.5]
            ranks = (ranks - 1) / (len(valid_data) - 1) - 0.5
            # 创建结果 Series，保持原始索引
            result = pd.Series(index=group.index)
            result.loc[valid_data.index] = ranks
            # 缺失值填充为 0
            result.fillna(0, inplace=True)
            return result

        # 按日期分组，对每个日期进行排名
        result = series.groupby('date').apply(rank_group)
        # 如果结果有多余的索引级别，删除它
        if isinstance(result.index, pd.MultiIndex) and len(result.index.names) > 2:
            result = result.droplevel(0)
        return result

    def RETURNS(self, close: pd.Series) -> pd.Series:
        """计算收益率

        这个函数就像一个"收益率计算器"，它会计算每只股票的日收益率。

        为什么需要这个函数？
        --------------------

        在因子分析中，收益率是最常用的指标之一：
        - 动量因子：基于历史收益率
        - 反转因子：基于短期收益率
        - 风险因子：基于收益率的波动

        这个函数提供了统一的收益率计算方法。

        工作原理（简单理解）
        ------------------

        就像计算股票涨跌幅：

        1. **按股票分组**：将数据按股票代码分组（就像分别计算每只股票）
        2. **按日期排序**：确保数据按时间顺序排列（就像按时间顺序计算）
        3. **计算涨跌幅**：使用 `pct_change()` 计算百分比变化（就像计算涨跌幅）
        4. **处理首日**：第一天的收益率设为 0（因为没有前一天的数据）

        实际使用场景
        -----------

        计算收盘价的收益率：

        ```python
        returns = self.RETURNS(factors['close'])
        # 返回的 Series 中，每个 (date, symbol) 对应的是该股票在该日的收益率
        ```

        Args:
            close: 收盘价 Series，索引为 (date, symbol) 多级索引

        Returns:
            pd.Series: 收益率 Series，索引为 (date, symbol)，第一天的收益率为 0

        Example:
            >>> factor = MyFactor()
            >>> returns = factor.RETURNS(factors['close'])
            >>> print(returns.head())
        """
        def calculate_returns(group):
            """计算单只股票的收益率"""
            # 按日期排序，确保时间顺序
            group = group.sort_index(level='date')
            # 计算百分比变化（收益率）
            result = group.pct_change()
            # 第一天的收益率设为 0（因为没有前一天的数据）
            result.iloc[0] = 0
            return result

        # 按股票代码分组，对每只股票分别计算收益率
        result = close.groupby(level='symbol', group_keys=False).apply(calculate_returns)
        return result

    def STDDEV(self, series: pd.Series, window: int = 20) -> pd.Series:
        """计算滚动标准差

        这个函数就像一个"波动率计算器"，它会计算每只股票在指定窗口内的滚动标准差。

        为什么需要这个函数？
        --------------------

        在因子分析中，标准差（波动率）是重要的风险指标：
        - 风险因子：基于收益率的波动
        - 质量因子：基于财务指标的稳定性
        - 技术因子：基于价格波动的特征

        这个函数提供了统一的滚动标准差计算方法。

        工作原理（简单理解）
        ------------------

        就像计算股票波动率：

        1. **按股票分组**：将数据按股票代码分组（就像分别计算每只股票）
        2. **按日期排序**：确保数据按时间顺序排列
        3. **滚动计算**：对每个窗口内的数据计算标准差（就像计算过去N天的波动率）
        4. **处理边界**：如果数据不足，使用最小周期要求（至少需要2个数据点或窗口的1/4）

        实际使用场景
        -----------

        计算收益率的20日滚动标准差：

        ```python
        returns = self.RETURNS(factors['close'])
        volatility = self.STDDEV(returns, window=20)
        # 返回的 Series 中，每个 (date, symbol) 对应的是该股票过去20天的收益率标准差
        ```

        Args:
            series: 要计算标准差的 Series，索引为 (date, symbol) 多级索引
            window: 滚动窗口大小，默认 20（即计算过去20天的标准差）

        Returns:
            pd.Series: 滚动标准差 Series，索引为 (date, symbol)

        Example:
            >>> factor = MyFactor()
            >>> returns = factor.RETURNS(factors['close'])
            >>> volatility = factor.STDDEV(returns, window=20)
        """
        def rolling_std(group):
            """计算单只股票的滚动标准差"""
            # 按日期排序，确保时间顺序
            group = group.sort_index(level='date')
            # 计算滚动标准差
            # min_periods: 最小周期要求，至少需要 max(2, window//4) 个数据点
            result = group.rolling(window=window, min_periods=max(2, window // 4)).std()
            return result

        # 按股票代码分组，对每只股票分别计算滚动标准差
        result = series.groupby(level='symbol', group_keys=False).apply(rolling_std)
        return result

    def CORRELATION(self, series1: pd.Series, series2: pd.Series, window: int = 20) -> pd.Series:
        """计算滚动相关系数

        这个函数就像一个"相关性计算器"，它会计算两个序列在指定窗口内的滚动相关系数。

        为什么需要这个函数？
        --------------------

        在因子分析中，相关性是重要的关系指标：
        - 因子相关性：分析不同因子之间的关系
        - 价格相关性：分析不同股票价格之间的关系
        - 指标相关性：分析不同财务指标之间的关系

        这个函数提供了统一的滚动相关系数计算方法。

        工作原理（简单理解）
        ------------------

        就像计算两个指标的相关性：

        1. **按股票分组**：分别计算每只股票的两个序列之间的相关性
        2. **对齐数据**：确保两个序列的索引对齐（就像确保时间点对应）
        3. **滚动计算**：对每个窗口内的数据计算相关系数（就像计算过去N天的相关性）

        实际使用场景
        -----------

        计算收益率和成交量的相关性：

        ```python
        returns = self.RETURNS(factors['close'])
        volume = factors['volume']
        corr = self.CORRELATION(returns, volume, window=20)
        # 返回的 Series 中，每个 (date, symbol) 对应的是该股票过去20天的收益率和成交量的相关系数
        ```

        Args:
            series1: 第一个序列，索引为 (date, symbol) 多级索引
            series2: 第二个序列，索引为 (date, symbol) 多级索引
            window: 滚动窗口大小，默认 20

        Returns:
            pd.Series: 滚动相关系数 Series，索引为 (date, symbol)，值在 [-1, 1] 之间

        Example:
            >>> factor = MyFactor()
            >>> returns = factor.RETURNS(factors['close'])
            >>> volume = factors['volume']
            >>> corr = factor.CORRELATION(returns, volume, window=20)
        """
        result = pd.Series(index=series1.index, dtype=float)
        # 遍历每只股票，分别计算相关性
        for symbol in series1.index.get_level_values('symbol').unique():
            # 提取该股票的两个序列
            s1 = series1[series1.index.get_level_values('symbol') == symbol]
            s2 = series2[series2.index.get_level_values('symbol') == symbol]
            # 对齐索引，确保时间点对应
            s1, s2 = s1.align(s2)
            # 计算滚动相关系数
            corr = s1.rolling(window=window).corr(s2)
            # 将结果保存到结果 Series 中
            result[s1.index] = corr
        return result

    def IF(self, condition, true_value, false_value):
        """条件选择函数

        这个函数就像一个"条件判断器"，它会根据条件选择返回 true_value 或 false_value。
        类似于 Excel 的 IF 函数或 Python 的三元运算符。

        为什么需要这个函数？
        --------------------

        在因子计算中，经常需要根据条件选择不同的值：
        - 如果收益率大于0，返回1，否则返回-1
        - 如果成交量大于平均值，返回成交量，否则返回0
        - 如果价格突破，返回信号，否则返回0

        这个函数提供了统一的条件选择方法。

        工作原理（简单理解）
        ------------------

        就像做选择题：

        1. **检查条件**：检查每个位置的条件是否为真（就像检查每道题是否答对）
        2. **选择值**：如果条件为真，选择 true_value；否则选择 false_value（就像选择答案）
        3. **返回结果**：返回选择后的 Series（就像返回答案）

        实际使用场景
        -----------

        根据收益率正负返回信号：

        ```python
        returns = self.RETURNS(factors['close'])
        signal = self.IF(returns > 0, 1, -1)
        # 如果收益率 > 0，返回 1；否则返回 -1
        ```

        根据成交量是否大于平均值选择值：

        ```python
        volume = factors['volume']
        volume_mean = volume.groupby('date').mean()
        volume_aligned = volume_mean.reindex(volume.index, level='date')
        result = self.IF(volume > volume_aligned, volume, 0)
        # 如果成交量大于平均值，返回成交量；否则返回 0
        ```

        Args:
            condition: 条件 Series，布尔类型，索引为 (date, symbol)
            true_value: 条件为真时的返回值（可以是标量或 Series）
            false_value: 条件为假时的返回值（可以是标量或 Series）

        Returns:
            pd.Series: 根据条件选择后的 Series，索引与 condition 相同

        Example:
            >>> factor = MyFactor()
            >>> returns = factor.RETURNS(factors['close'])
            >>> signal = factor.IF(returns > 0, 1, -1)
        """
        # 使用 numpy.where 根据条件选择值
        # np.where(condition, true_value, false_value) 类似于三元运算符
        return pd.Series(np.where(condition, true_value, false_value), index=condition.index)

    def DELAY(self, series: pd.Series, period: int = 1) -> pd.Series:
        """计算滞后值

        这个函数就像一个"时间机器"，它会将数据向前移动指定的期数，
        返回过去某个时间点的值。

        为什么需要这个函数？
        --------------------

        在因子计算中，经常需要使用历史数据：
        - 动量因子：使用过去N天的收益率
        - 反转因子：使用过去的价格
        - 技术指标：使用历史的成交量

        这个函数提供了统一的滞后值计算方法。

        工作原理（简单理解）
        ------------------

        就像查看历史记录：

        1. **按股票分组**：将数据按股票代码分组（就像分别查看每只股票的历史）
        2. **向前移动**：将数据向前移动 period 期（就像查看过去 period 天的数据）
        3. **返回结果**：返回移动后的 Series（就像返回历史数据）

        实际使用场景
        -----------

        获取前一天的收盘价：

        ```python
        close = factors['close']
        close_lag1 = self.DELAY(close, period=1)
        # 返回的 Series 中，每个 (date, symbol) 对应的是该股票前一天的收盘价
        ```

        获取过去5天的收盘价：

        ```python
        close_lag5 = self.DELAY(close, period=5)
        # 返回的 Series 中，每个 (date, symbol) 对应的是该股票5天前的收盘价
        ```

        注意事项
        --------

        - period=1 表示向前移动1期，即获取前1天的值
        - 第一天的滞后值会是 NaN（因为没有更早的数据）
        - 每只股票的滞后值是独立计算的

        Args:
            series: 要计算滞后值的 Series，索引为 (date, symbol) 多级索引
            period: 滞后期数，默认 1（即获取前1天的值）

        Returns:
            pd.Series: 滞后值 Series，索引为 (date, symbol)，前 period 天的值为 NaN

        Example:
            >>> factor = MyFactor()
            >>> close = factors['close']
            >>> close_lag1 = factor.DELAY(close, period=1)
        """
        # 按股票代码分组，对每只股票分别计算滞后值
        # shift(period) 将数据向前移动 period 期
        return series.groupby(level='symbol').shift(period)

    def SUM(self, series: pd.Series, window: int = 20) -> pd.Series:
        """计算滚动和

        这个函数就像一个"累加器"，它会计算每只股票在指定窗口内的滚动和。

        为什么需要这个函数？
        --------------------

        在因子计算中，经常需要计算滚动和：
        - 动量因子：计算过去N天的收益率总和
        - 成交量因子：计算过去N天的成交量总和
        - 资金流因子：计算过去N天的资金流入总和

        这个函数提供了统一的滚动和计算方法。

        工作原理（简单理解）
        ------------------

        就像计算累计值：

        1. **按股票分组**：将数据按股票代码分组（就像分别计算每只股票）
        2. **滚动累加**：对每个窗口内的数据求和（就像计算过去N天的总和）
        3. **返回结果**：返回滚动和 Series（就像返回累计值）

        实际使用场景
        -----------

        计算过去20天的收益率总和：

        ```python
        returns = self.RETURNS(factors['close'])
        momentum = self.SUM(returns, window=20)
        # 返回的 Series 中，每个 (date, symbol) 对应的是该股票过去20天的收益率总和
        ```

        计算过去5天的成交量总和：

        ```python
        volume = factors['volume']
        volume_sum = self.SUM(volume, window=5)
        # 返回的 Series 中，每个 (date, symbol) 对应的是该股票过去5天的成交量总和
        ```

        注意事项
        --------

        - min_periods=1 表示至少需要1个数据点就可以计算（即使窗口未满）
        - 如果数据不足，会使用可用的数据计算部分和

        Args:
            series: 要计算滚动和的 Series，索引为 (date, symbol) 多级索引
            window: 滚动窗口大小，默认 20（即计算过去20天的和）

        Returns:
            pd.Series: 滚动和 Series，索引为 (date, symbol)

        Example:
            >>> factor = MyFactor()
            >>> returns = factor.RETURNS(factors['close'])
            >>> momentum = factor.SUM(returns, window=20)
        """
        # 按股票代码分组，对每只股票分别计算滚动和
        # rolling(window).sum() 计算滚动窗口内的和
        # min_periods=1 表示至少需要1个数据点就可以计算
        # droplevel(0) 删除分组产生的额外索引级别
        return series.groupby(level='symbol').rolling(window=window, min_periods=1).sum().droplevel(0)