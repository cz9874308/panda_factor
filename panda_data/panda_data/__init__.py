"""
数据读取模块

本模块提供了统一的数据读取接口，就像一个"数据访问中心"，
它封装了市场数据和因子数据的读取功能，让你可以方便地从 MongoDB 数据库中
获取股票市场数据和因子数据，而不需要关心底层的数据库操作细节。

核心概念
--------

- **市场数据**：股票的开盘价、收盘价、成交量等基础数据
- **因子数据**：计算好的因子值数据
- **自定义因子**：用户创建的因子数据
- **分钟数据**：股票的分钟级交易数据

使用方式
--------

1. 调用 `init()` 初始化模块（必须在使用前调用）
2. 使用各种 `get_*` 函数获取数据

工作原理
--------

就像图书馆的借阅系统：

1. **初始化**：调用 `init()` 建立数据库连接（就像图书馆开门）
2. **查询数据**：使用各种函数查询数据（就像借阅图书）
3. **返回结果**：以 DataFrame 格式返回数据（就像把书给借阅者）

注意事项
--------

- 使用前必须先调用 `init()` 初始化模块
- 所有日期格式为 YYYYMMDD（如 "20240101"）
- 返回的数据格式为 pandas DataFrame
- 如果没有找到数据，返回 None
"""

import logging
from typing import Optional, List, Union

import pandas as pd

from panda_common.config import get_config
from panda_data.factor.factor_reader import FactorReader
from panda_data.market_data.market_data_reader import MarketDataReader
from panda_data.market_data.market_stock_cn_minute_reader import MarketStockCnMinReaderV3

# 模块级全局变量，用于存储初始化的组件
# 使用懒加载模式，只有在调用 init() 时才初始化
_config = None  # 配置对象
_factor = None  # 因子数据读取器
_market_data = None  # 市场数据读取器
_market_min_data: MarketStockCnMinReaderV3 = None  # 分钟数据读取器


def init(configPath: Optional[str] = None) -> None:
    """初始化数据读取模块

    这个函数就像一个"数据访问中心的启动器"，它会初始化所有数据读取器，
    建立数据库连接，让你可以开始使用各种数据读取功能。

    为什么需要这个函数？
    --------------------

    在使用数据读取功能之前，需要：
    - 加载配置信息
    - 创建数据库连接
    - 初始化各种数据读取器

    如果每个函数都自己初始化，会导致：
    - 重复初始化，浪费资源
    - 连接管理混乱
    - 性能下降

    这个函数统一管理初始化过程，解决了这些问题。

    工作原理（简单理解）
    ------------------

    就像打开图书馆的门：

    1. **加载配置**：从配置文件加载数据库连接信息（就像拿到图书馆的钥匙）
    2. **创建连接**：初始化各种数据读取器（就像打开各个书库的门）
    3. **准备就绪**：所有组件准备完成，可以开始使用（就像图书馆可以借书了）

    实际使用场景
    -----------

    在应用启动时或使用数据读取功能前调用：

    ```python
    import panda_data

    # 初始化模块
    panda_data.init()

    # 现在可以使用各种数据读取功能
    data = panda_data.get_market_data("20240101", "20241231")
    ```

    可能遇到的问题
    ------------

    配置加载失败
    ^^^^^^^^^^^

    如果配置文件不存在或格式错误，会抛出 `RuntimeError` 异常。
    确保配置文件存在且格式正确。

    数据库连接失败
    ^^^^^^^^^^^^^

    如果数据库服务未启动或配置错误，会抛出 `RuntimeError` 异常。
    确保 MongoDB 服务正常运行，并且配置信息正确。

    Args:
        configPath: 配置文件路径（可选），如果为 None 则使用默认配置
                    注意：当前实现忽略此参数，统一使用 panda_common 的配置

    Raises:
        RuntimeError: 如果配置加载失败或初始化失败

    Example:
        >>> import panda_data
        >>> panda_data.init()
        >>> # 现在可以使用数据读取功能了
    """
    global _config, _factor, _market_data, _market_min_data

    try:
        # 使用 panda_common 中的配置
        # 统一使用项目的配置管理，确保配置一致性
        _config = get_config()

        if not _config:
            raise RuntimeError("Failed to load configuration from panda_common")

        # 初始化各种数据读取器
        # 这些读取器会建立数据库连接，准备读取数据
        _factor = FactorReader(_config)  # 因子数据读取器
        _market_data = MarketDataReader(_config)  # 市场数据读取器
        _market_min_data = MarketStockCnMinReaderV3(_config)  # 分钟数据读取器
    except Exception as e:
        raise RuntimeError(f"Failed to initialize panda_data: {str(e)}")


def get_all_symbols() -> pd.DataFrame:
    """获取所有股票代码

    这个函数就像一个"股票代码目录"，它会返回数据库中所有可用的股票代码。

    为什么需要这个函数？
    --------------------

    在因子分析中，经常需要知道有哪些股票可以分析。
    这个函数提供了快速获取所有股票代码的能力。

    工作原理
    --------

    1. 检查模块是否已初始化
    2. 从分钟数据读取器获取所有股票代码
    3. 返回 DataFrame 格式的股票代码列表

    Returns:
        pd.DataFrame: 包含所有股票代码的 DataFrame

    Raises:
        RuntimeError: 如果模块未初始化

    Example:
        >>> panda_data.init()
        >>> symbols = panda_data.get_all_symbols()
        >>> print(f"共有 {len(symbols)} 个股票代码")
    """
    if _market_min_data is None:
        raise RuntimeError("Please call init() before using any functions")
    return _market_min_data.get_all_symbols()


def get_factor(
        factors: Union[str, List[str]],
        start_date: str,
        end_date: str,
        symbols: Optional[Union[str, List[str]]] = None,
        index_component: Optional[str] = None,
        type: Optional[str] = 'stock'
) -> Optional[pd.DataFrame]:
    """获取因子数据

    这个函数就像一个"因子数据查询器"，它会根据指定的因子名称、时间范围和股票代码，
    从数据库中查询并返回因子数据。

    为什么需要这个函数？
    --------------------

    在因子分析中，需要获取各种因子的数据：
    - 基础因子：如 open、close、volume 等
    - 计算因子：如动量因子、反转因子等

    这个函数提供了统一的接口来获取这些数据，而不需要关心数据存储的细节。

    工作原理（简单理解）
    ------------------

    就像在图书馆找书：

    1. **指定书名**：提供因子名称（就像指定要找的书名）
    2. **指定时间**：提供时间范围（就像指定要哪个版本的书）
    3. **指定位置**：提供股票代码（就像指定在哪个书架）
    4. **查找返回**：从数据库查询并返回数据（就像找到书并给你）

    实际使用场景
    -----------

    获取基础因子数据：

    ```python
    # 获取收盘价数据
    close_data = panda_data.get_factor(
        factors="close",
        start_date="20240101",
        end_date="20241231",
        symbols=["000001", "000002"]
    )
    ```

    获取多个因子数据：

    ```python
    # 获取多个基础因子
    factors_data = panda_data.get_factor(
        factors=["close", "volume", "open"],
        start_date="20240101",
        end_date="20241231"
    )
    ```

    Args:
        factors: 因子名称，可以是单个字符串或字符串列表
                 基础因子：如 "close"、"open"、"volume" 等
        start_date: 开始日期，格式 YYYYMMDD，如 "20240101"
        end_date: 结束日期，格式 YYYYMMDD，如 "20241231"
        symbols: 股票代码列表或单个代码，如果为 None 则返回所有股票
        index_component: 指数成分股过滤，如 "100"（沪深300）、"010"（中证500）
        type: 数据类型，'stock' 表示股票，'future' 表示期货

    Returns:
        Optional[pd.DataFrame]: 因子数据 DataFrame，索引为 (date, symbol) 多级索引
                                如果未找到数据，返回 None

    Raises:
        RuntimeError: 如果模块未初始化

    Example:
        >>> panda_data.init()
        >>> data = panda_data.get_factor(
        ...     factors="close",
        ...     start_date="20240101",
        ...     end_date="20241231",
        ...     symbols=["000001"]
        ... )
        >>> print(data.head())
    """
    if _factor is None:
        raise RuntimeError("Please call init() before using any functions")

    return _factor.get_factor(symbols, factors, start_date, end_date, index_component, type)


def get_custom_factor(
        factor_logger: logging.Logger,
        user_id: int,
        factor_name: str,
        start_date: str,
        end_date: str,
        symbol_type: Optional[str] = 'stock'
) -> Optional[pd.DataFrame]:
    """获取用户自定义因子数据

    这个函数就像一个"自定义因子计算器"，它会获取用户创建的因子代码，
    执行计算，并返回因子数据。

    为什么需要这个函数？
    --------------------

    用户创建的因子有两种情况：
    1. **已计算并持久化**：因子值已经计算好并保存到数据库，直接读取
    2. **未计算或需要重新计算**：需要执行因子代码进行计算

    这个函数会自动判断情况，如果已持久化就直接读取，否则执行计算。

    工作原理（简单理解）
    ------------------

    就像处理定制订单：

    1. **检查库存**：检查因子是否已经计算并保存（就像检查是否有现成的产品）
       - 如果有：直接返回保存的数据（就像直接发货）
       - 如果没有：继续下一步

    2. **获取订单**：从数据库获取因子的代码和参数（就像获取订单详情）

    3. **执行生产**：执行因子代码，计算因子值（就像按照订单生产产品）

    4. **返回产品**：返回计算好的因子数据（就像发货）

    实际使用场景
    -----------

    在因子分析时使用：

    ```python
    import logging
    logger = logging.getLogger("factor_analysis")

    # 获取用户创建的因子数据
    factor_data = panda_data.get_custom_factor(
        factor_logger=logger,
        user_id=123,
        factor_name="my_momentum_factor",
        start_date="20240101",
        end_date="20241231"
    )
    ```

    可能遇到的问题
    ------------

    因子代码错误
    ^^^^^^^^^^^

    如果因子代码有语法错误或运行时错误，函数会记录详细的错误信息到日志，
    并返回 None。检查日志可以找到具体的错误位置。

    因子不存在
    ^^^^^^^^^^^

    如果指定的因子不存在，函数会返回 None。
    确保因子已经创建并且因子名称正确。

    Args:
        factor_logger: 日志记录器，用于记录因子计算过程中的日志
        user_id: 用户 ID，用于标识因子属于哪个用户
        factor_name: 因子名称，用于查找因子定义
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        symbol_type: 数据类型，'stock' 表示股票，'future' 表示期货

    Returns:
        Optional[pd.DataFrame]: 因子数据 DataFrame，索引为 (date, symbol) 多级索引
                                如果因子不存在或计算失败，返回 None

    Raises:
        RuntimeError: 如果模块未初始化

    Example:
        >>> import logging
        >>> logger = logging.getLogger("test")
        >>> panda_data.init()
        >>> data = panda_data.get_custom_factor(
        ...     factor_logger=logger,
        ...     user_id=123,
        ...     factor_name="my_factor",
        ...     start_date="20240101",
        ...     end_date="20241231"
        ... )
    """
    if _factor is None:
        raise RuntimeError("Please call init() before using any functions")

    return _factor.get_custom_factor(factor_logger, user_id, factor_name, start_date, end_date, symbol_type)

def get_factor_by_name(factor_name, start_date, end_date):
    """根据因子名称获取因子数据

    这个函数会根据因子名称查找因子定义，执行计算，并返回因子数据。
    与 `get_custom_factor()` 的区别是，这个函数不需要指定用户 ID，
    适用于系统因子或公开因子。

    为什么需要这个函数？
    --------------------

    有些因子可能是系统提供的或公开的，不需要指定用户 ID。
    这个函数提供了更简单的接口来获取这些因子。

    工作原理
    --------

    1. 根据因子名称查找因子定义
    2. 执行因子代码计算因子值
    3. 返回计算结果

    Args:
        factor_name: 因子名称
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD

    Returns:
        Optional[pd.DataFrame]: 因子数据 DataFrame，如果因子不存在或计算失败返回 None

    Raises:
        RuntimeError: 如果模块未初始化

    Example:
        >>> panda_data.init()
        >>> data = panda_data.get_factor_by_name(
        ...     "momentum_factor",
        ...     "20240101",
        ...     "20241231"
        ... )
    """
    if _factor is None:
        raise RuntimeError("Please call init() before using any functions")
    return _factor.get_factor_by_name(factor_name, start_date, end_date)


def get_stock_instruments() -> pd.DataFrame:
    """获取所有股票的基本信息

    这个函数会返回所有股票的基本信息，包括股票代码、名称等。

    为什么需要这个函数？
    --------------------

    在因子分析中，经常需要获取股票的基本信息，比如股票名称。
    这个函数提供了快速获取所有股票信息的能力。

    工作原理
    --------

    1. 从分钟数据读取器获取所有股票信息
    2. 转换为 DataFrame 格式返回

    Returns:
        pd.DataFrame: 包含所有股票基本信息的 DataFrame

    Raises:
        RuntimeError: 如果模块未初始化

    Example:
        >>> panda_data.init()
        >>> stocks = panda_data.get_stock_instruments()
        >>> print(stocks.head())
    """
    if _market_min_data is None:
        raise RuntimeError("Please call init() before using any functions")
    stocks = _market_min_data.get_stock_instruments()
    return pd.DataFrame(stocks)


def get_market_min_data(
        start_date: str,
        end_date: str,
        symbol: Optional[str] = None,
        fields: Optional[Union[str, List[str]]] = None
) -> Optional[pd.DataFrame]:
    """获取股票分钟级市场数据

    这个函数就像一个"分钟数据查询器"，它会从数据库中查询股票的分钟级交易数据，
    包括每分钟的开盘价、收盘价、最高价、最低价、成交量等。

    为什么需要这个函数？
    --------------------

    在量化分析中，有时需要分钟级的数据进行更精细的分析：
    - 日内交易策略需要分钟数据
    - 高频因子计算需要分钟数据
    - 实时监控需要最新的分钟数据

    这个函数提供了获取分钟数据的能力。

    工作原理
    --------

    就像查询详细的交易记录：

    1. **指定股票**：提供股票代码（就像指定要查哪只股票）
    2. **指定时间**：提供时间范围（就像指定要查哪个时间段）
    3. **指定字段**：提供需要的字段（就像指定要查哪些信息）
    4. **查询返回**：从数据库查询并返回数据（就像返回交易记录）

    实际使用场景
    -----------

    获取单只股票的分钟数据：

    ```python
    # 获取 000001 在 2024年1月1日 的分钟数据
    minute_data = panda_data.get_market_min_data(
        start_date="20240101",
        end_date="20240101",
        symbol="000001",
        fields=["open", "close", "volume"]
    )
    ```

    Args:
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        symbol: 股票代码，如果为 None 则返回所有股票
        fields: 字段列表，如 ["open", "close", "volume"]
                如果为 None 则返回所有可用字段

    Returns:
        Optional[pd.DataFrame]: 分钟数据 DataFrame，如果未找到数据返回 None

    Raises:
        RuntimeError: 如果模块未初始化

    Example:
        >>> panda_data.init()
        >>> data = panda_data.get_market_min_data(
        ...     start_date="20240101",
        ...     end_date="20240101",
        ...     symbol="000001"
        ... )
    """
    if _market_min_data is None:
        raise RuntimeError("Please call init() before using any functions")

    return _market_min_data.get_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        fields=fields
    )


def get_market_data(
        start_date: str,
        end_date: str,
        indicator="000985",
        st=True,
        symbols: Optional[Union[str, List[str]]] = None,
        fields: Optional[Union[str, List[str]]] = None
) -> Optional[pd.DataFrame]:
    """获取股票日级市场数据

    这个函数就像一个"市场数据查询器"，它会从数据库中查询股票的日级交易数据，
    包括每日的开盘价、收盘价、最高价、最低价、成交量等，支持并行查询提高性能。

    为什么需要这个函数？
    --------------------

    在因子分析中，需要获取大量的市场数据：
    - 计算收益率需要收盘价数据
    - 计算波动率需要价格数据
    - 计算成交量因子需要成交量数据

    如果数据量很大，单线程查询会很慢。
    这个函数使用并行查询，可以大大提高查询速度。

    工作原理（简单理解）
    ------------------

    就像多人同时找书：

    1. **分割任务**：将时间范围分成多个小块（就像把找书任务分给多个人）
    2. **并行查询**：多个线程同时查询不同时间段的数据（就像多人同时找书）
    3. **合并结果**：将所有结果合并成一个 DataFrame（就像把找到的书汇总）

    实际使用场景
    -----------

    获取多只股票的市场数据：

    ```python
    # 获取沪深300成分股在2024年的市场数据
    market_data = panda_data.get_market_data(
        start_date="20240101",
        end_date="20241231",
        indicator="000300",  # 沪深300
        st=False,  # 不包含ST股票
        fields=["open", "close", "high", "low", "volume"]
    )
    ```

    参数说明
    --------

    - **indicator**：股票池代码
      - "000985"：全A股（默认）
      - "000300"：沪深300
      - "000905"：中证500
      - "000852"：中证1000
    - **st**：是否包含ST股票，True 表示包含，False 表示不包含

    Args:
        start_date: 开始日期，格式 YYYYMMDD
        end_date: 结束日期，格式 YYYYMMDD
        indicator: 股票池代码，默认 "000985"（全A股）
        st: 是否包含ST股票，默认 True
        symbols: 股票代码列表或单个代码，如果为 None 则根据 indicator 和 st 过滤
        fields: 字段列表，如 ["open", "close", "volume"]
                如果为 None 则返回所有可用字段

    Returns:
        Optional[pd.DataFrame]: 市场数据 DataFrame，包含 date、symbol 等列
                                如果未找到数据，返回 None

    Raises:
        RuntimeError: 如果模块未初始化

    Example:
        >>> panda_data.init()
        >>> data = panda_data.get_market_data(
        ...     start_date="20240101",
        ...     end_date="20241231",
        ...     indicator="000300",
        ...     st=False
        ... )
        >>> print(data.head())
    """
    if _market_data is None:
        raise RuntimeError("Please call init() before using any functions")

    return _market_data.get_market_data(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        indicator=indicator,
        st=st,
        fields=fields
    )


def get_available_market_fields() -> List[str]:
    """获取所有可用的市场数据字段

    这个函数就像一个"字段目录"，它会返回数据库中所有可用的市场数据字段名称。

    为什么需要这个函数？
    --------------------

    在查询市场数据时，需要知道有哪些字段可用。
    这个函数提供了查看所有可用字段的能力，方便：
    - 了解数据结构
    - 选择合适的字段
    - 验证字段名称是否正确

    工作原理
    --------

    1. 检查模块是否已初始化
    2. 从市场数据读取器获取所有可用字段
    3. 返回字段名称列表

    Returns:
        List[str]: 所有可用字段的名称列表，如 ["open", "close", "high", "low", "volume"]

    Raises:
        RuntimeError: 如果模块未初始化

    Example:
        >>> panda_data.init()
        >>> fields = panda_data.get_available_market_fields()
        >>> print(fields)
        ['open', 'close', 'high', 'low', 'volume', ...]
    """
    if _market_data is None:
        raise RuntimeError("Please call init() before using any functions")

    return _market_data.get_available_fields()