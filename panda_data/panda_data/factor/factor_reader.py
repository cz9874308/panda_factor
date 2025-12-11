"""
因子数据读取模块

本模块提供了一个因子数据读取器，它能够从 MongoDB 数据库中读取各种类型的因子数据，
包括基础因子（如 open、close、volume）和用户自定义因子（公式因子、Python类因子）。

核心概念
--------

- **基础因子**：系统预定义的因子，如开盘价、收盘价、成交量等
- **自定义因子**：用户创建的因子，可以是公式或 Python 类
- **公式因子**：使用公式表达式定义的因子，如 "close / open - 1"
- **Python类因子**：使用 Python 类定义的因子，继承自 Factor 基类

为什么需要这个模块？
-------------------

在量化分析中，需要获取各种因子数据：
- 基础因子：用于计算其他因子
- 自定义因子：用户创建的复杂因子

这个模块提供了统一的接口来获取这些数据，并且支持：
- 从持久化表直接读取（如果因子已计算并保存）
- 动态计算因子（如果因子未计算或需要重新计算）

工作原理（简单理解）
------------------

就像图书馆的借阅系统：

1. **基础因子**：就像图书馆的常用书籍，已经整理好，直接借阅
2. **自定义因子**：
   - 如果已经计算并保存：就像已经装订好的书，直接借阅
   - 如果未计算：就像需要现场装订的书，先装订再借阅

注意事项
--------

- 自定义因子计算可能需要较长时间，特别是数据量大的时候
- 公式因子和 Python 类因子在计算时如果出错，会记录详细的错误信息
- 因子数据会以 DataFrame 格式返回，索引为 (date, symbol)
"""

import logging
from typing import Optional

import pandas as pd
import time
import traceback

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.models.requestEntity import FactorsRequest


class FactorReader:
    """因子数据读取器

    这个类就像一个"因子数据查询中心"，它提供了从 MongoDB 数据库读取各种因子数据的能力，
    包括基础因子和用户自定义因子。

    为什么需要这个类？
    -----------------

    在量化分析中，需要获取各种因子数据：
    - 基础因子：如 open、close、volume 等
    - 自定义因子：用户创建的公式因子或 Python 类因子

    如果每次查询都直接访问数据库，会导致：
    - 代码重复
    - 错误处理不统一
    - 性能问题

    这个类统一管理因子数据的读取，解决了这些问题。

    工作原理（简单理解）
    ------------------

    就像图书馆的借阅系统：

    1. **接收请求**：接收因子查询请求（就像接收借书请求）
    2. **判断类型**：判断是基础因子还是自定义因子（就像判断是常用书还是定制书）
    3. **获取数据**：
       - 基础因子：直接从数据库读取（就像直接借阅常用书）
       - 自定义因子：
         - 如果已计算：从持久化表读取（就像借阅已装订的书）
         - 如果未计算：执行计算（就像现场装订书）
    4. **返回结果**：返回因子数据 DataFrame（就像把书给借阅者）

    实际使用场景
    -----------

    读取基础因子：

    ```python
    reader = FactorReader(config)
    data = reader.get_factor(
        symbols=["000001"],
        factors=["close", "volume"],
        start_date="20240101",
        end_date="20241231"
    )
    ```

    读取自定义因子：

    ```python
    data = reader.get_custom_factor(
        factor_logger=logger,
        user_id=123,
        factor_name="my_factor",
        start_date="20240101",
        end_date="20241231"
    )
    ```

    注意事项
    --------

    - 使用前需要先初始化，传入配置对象
    - 自定义因子计算可能需要较长时间
    - 如果因子代码有错误，会记录详细的错误信息到日志
    """
    def __init__(self, config):
        """初始化因子数据读取器

        这个函数就像"启动因子查询中心"，它会：
        - 保存配置信息
        - 创建数据库连接
        - 预加载所有股票代码（用于后续查询）

        为什么需要预加载股票代码？
        ------------------------

        在查询因子数据时，如果用户没有指定股票代码，需要返回所有股票的数据。
        如果每次都去数据库查询所有股票代码，会很慢。
        所以在初始化时预加载一次，后续直接使用，提高效率。

        工作原理
        --------

        1. **保存配置**：保存传入的配置对象，用于后续数据库连接
        2. **创建连接**：创建数据库处理器，建立数据库连接
        3. **预加载代码**：从数据库获取所有股票代码，保存到内存中

        Args:
            config: 配置字典，必须包含 MongoDB 连接信息（如 MONGO_DB）

        Raises:
            RuntimeError: 如果数据库连接失败或无法获取股票代码

        Example:
            >>> from panda_common.config import get_config
            >>> config = get_config()
            >>> reader = FactorReader(config)
        """
        self.config = config
        # 初始化数据库处理器，用于后续的数据库操作
        self.db_handler = DatabaseHandler(config)
        # 预加载所有股票代码，避免每次查询时都要查询数据库
        self.all_symbols = self.get_all_symbols()

    def _print_formula_error(self, e, formula, factor_logger: logging.Logger):
        """打印公式因子的错误信息

        这个函数就像一个"错误诊断专家"，它会分析公式因子的错误，
        并输出详细的错误信息，帮助用户快速定位问题。

        为什么需要这个函数？
        --------------------

        公式因子在计算时可能会出错：
        - 语法错误：公式表达式有语法问题
        - 执行错误：公式在执行时出错
        - 设置错误：公式设置时出错

        如果只是简单地打印错误信息，用户很难找到问题所在。
        这个函数会分析错误类型，输出详细的错误信息，包括：
        - 错误位置（行号、偏移量）
        - 错误代码片段
        - 错误类型

        工作原理
        --------

        1. **判断错误类型**：判断是语法错误还是执行错误
        2. **提取错误信息**：从异常对象中提取错误信息
        3. **定位错误位置**：找到错误发生的具体位置
        4. **输出详细信息**：将错误信息记录到日志

        Args:
            e: 异常对象
            formula: 公式字符串
            factor_logger: 日志记录器，用于输出错误信息

        Example:
            >>> try:
            ...     # 执行公式计算
            ... except Exception as e:
            ...     reader._print_formula_error(e, "close / open", logger)
        """
        if isinstance(e, SyntaxError):
            factor_logger.error("\n=== Formula Syntax Error ===")
            factor_logger.error(f"Error in formula: {formula}")
            factor_logger.error(f"Error message: {str(e)}")
            factor_logger.error(f"Error occurred at line {e.lineno}, offset {e.offset}")
            factor_logger.error(f"Text: {e.text}")
            return

        tb = traceback.extract_tb(e.__traceback__)
        if any('eval' in frame.name for frame in tb):
            # 公式执行错误
            factor_logger.error("\n=== Formula Execution Error ===")
            factor_logger.error(f"Error in formula: {formula}")
            factor_logger.error(f"Error message: {str(e)}")
            # 找到最后一个相关的错误帧
            last_frame = None
            for frame in reversed(tb):
                if 'eval' in frame.name:
                    last_frame = frame
                    break
            if last_frame:
                factor_logger.error(f"Error occurred at line {last_frame.lineno}")
                factor_logger.error(f"In expression: {last_frame.line}")
        else:
            # 公式设置错误
            factor_logger.error("\n=== Formula Setup Error ===")
            factor_logger.error(f"Error in formula setup: {str(e)}")
            factor_logger.error(f"Error type: {type(e)}")

    def _print_class_error(self, e, code, factor_logger):
        """打印类因子的错误信息

        这个函数就像一个"错误诊断专家"，它会分析 Python 类因子的错误，
        并输出详细的错误信息，帮助用户快速定位问题。

        为什么需要这个函数？
        --------------------

        Python 类因子在计算时可能会出错：
        - 语法错误：Python 代码有语法问题
        - 计算错误：calculate 方法执行时出错
        - 类错误：因子类定义或初始化时出错

        如果只是简单地打印错误信息，用户很难找到问题所在。
        这个函数会分析错误类型，输出详细的错误信息，包括：
        - 错误位置（行号）
        - 错误代码片段
        - 错误类型

        工作原理
        --------

        1. **判断错误类型**：判断是语法错误还是计算错误
        2. **提取错误信息**：从异常对象中提取错误信息
        3. **定位错误位置**：找到错误发生的具体位置（特别是 calculate 方法）
        4. **输出详细信息**：将错误信息记录到日志

        Args:
            e: 异常对象
            code: Python 代码字符串
            factor_logger: 日志记录器，用于输出错误信息

        Example:
            >>> try:
            ...     # 执行类因子计算
            ... except Exception as e:
            ...     reader._print_class_error(e, class_code, logger)
        """
        tb = traceback.extract_tb(e.__traceback__)

        if isinstance(e, SyntaxError):
            factor_logger.error("\n=== Python Syntax Error ===")
            factor_logger.error(f"Error message: {str(e)}")
            factor_logger.error(f"Error occurred at line {e.lineno}, offset {e.offset}")
            factor_logger.error(f"Text: {e.text}")
            return

        # 检查是否是计算方法中的错误
        calc_frame = None
        for frame in tb:
            if 'calculate' in frame.name:
                calc_frame = frame
                break

        if calc_frame:
            # 因子计算错误
            factor_logger.error("\n=== Factor Calculation Error ===")
            factor_logger.error(f"Error in factor calculation:")
            factor_logger.error(f"Error message: {str(e)}")
            factor_logger.error(f"Error occurred at line {calc_frame.lineno} in calculate method")
            factor_logger.error(f"In code: {calc_frame.line}")
        else:
            # 因子类错误
            factor_logger.error("\n=== Factor Class Error ===")
            factor_logger.error(f"Error in factor class execution: {str(e)}")
            factor_logger.error(f"Error type: {type(e)}")

    def get_factor(self, symbols, factors, start_date, end_date, index_component: Optional[str] = None,
                   type: Optional[str] = 'stock'):
        """获取因子数据

        这个函数就像一个"因子数据查询器"，它会根据指定的因子名称、时间范围和股票代码，
        从数据库中查询并返回因子数据。目前主要支持基础因子（如 open、close、volume）。

        为什么需要这个函数？
        --------------------

        在因子分析中，需要获取各种基础因子数据：
        - 计算收益率需要收盘价数据
        - 计算波动率需要价格数据
        - 计算成交量因子需要成交量数据

        这个函数提供了统一的接口来获取这些数据，而不需要关心数据存储的细节。

        工作原理（简单理解）
        ------------------

        就像在图书馆找书：

        1. **指定书名**：提供因子名称（就像指定要找的书名）
        2. **指定时间**：提供时间范围（就像指定要哪个版本的书）
        3. **指定位置**：提供股票代码（就像指定在哪个书架）
        4. **查找返回**：从数据库查询并返回数据（就像找到书并给你）

        性能优化
        --------

        - **批量查询**：如果查询多个基础因子，会一次性查询所有因子，然后选择需要的字段
        - **批量大小优化**：使用 batch_size=100000 来优化查询性能

        Args:
            symbols: 股票代码列表或单个代码，如果为 None 则返回所有股票
            factors: 因子名称，可以是单个字符串或字符串列表
                     支持的基础因子：open、close、high、low、volume、market_cap、turnover、amount
            start_date: 开始日期，格式 YYYYMMDD，如 "20240101"
            end_date: 结束日期，格式 YYYYMMDD，如 "20241231"
            index_component: 指数成分股过滤，如 "100"（沪深300）、"010"（中证500）
            type: 数据类型，'stock' 表示股票，'future' 表示期货（默认 'stock'）

        Returns:
            Optional[pd.DataFrame]: 因子数据 DataFrame，包含 date、symbol 和因子值列
                                    如果未找到数据，返回 None

        Raises:
            RuntimeError: 如果数据库连接失败

        Example:
            >>> reader = FactorReader(config)
            >>> data = reader.get_factor(
            ...     symbols=["000001"],
            ...     factors=["close", "volume"],
            ...     start_date="20240101",
            ...     end_date="20241231"
            ... )
            >>> print(data.head())
        """
        all_data = []
        # 将所有因子名称转换为小写，统一处理
        if isinstance(factors, str):
            factors = factors.lower()
        elif isinstance(factors, list):
            factors = [f.lower() for f in factors]
        
        # 检查是否有基础因子（系统预定义的因子）
        base_factors = ["open", "close", "high", "low", "volume", "market_cap", "turnover", "amount"]
        requested_base_factors = [f for f in factors if f in base_factors]

        # 如果有基础因子，一次性查询所有基础因子，然后选择需要的字段
        # 这样可以减少数据库查询次数，提高性能
        if requested_base_factors:
            # 单次查询 factor_base 表，获取所有需要的基础因子
            query = {
                "date": {"$gte": start_date, "$lte": end_date}
            }
            # 如果指定了指数成分股过滤，添加到查询条件中
            if index_component:
                query['index_component'] = {"$eq": index_component}

            # 构建投影，只查询需要的字段，减少数据传输量
            base_fields = ['date', 'symbol']  # 基础字段（必须包含）
            projection = {field: 1 for field in base_fields + requested_base_factors}
            projection['_id'] = 0  # 不包含 _id 字段

            if type == 'future':
                # 期货数据：需要特殊查询条件（symbol = underlying_symbol + "88"）
                query["$expr"] = {
                    "$eq": [
                        "$symbol",
                        {"$concat": ["$underlying_symbol", "88"]}
                    ]
                }
                # 获取期货市场数据集合
                collection = self.db_handler.get_mongo_collection(
                    self.config["MONGO_DB"],
                    "future_market"
                )
                # 使用大批量大小优化查询性能
                cursor = collection.find(query, projection).batch_size(100000)
                records = list(cursor)
            else:
                # 股票数据：使用 factor_base 集合
                collection = self.db_handler.get_mongo_collection(
                    self.config["MONGO_DB"],
                    "factor_base"
                )
                # 使用大批量大小优化查询性能
                cursor = collection.find(query, projection).batch_size(100000)
                records = list(cursor)

            if records:
                # 将查询结果转换为 DataFrame
                df = pd.DataFrame(records)
                all_data.append(df)

        # 如果没有任何数据，返回 None
        if not all_data:
            logger.warning(f"No data found for the specified parameters")
            return None

        # 合并所有 DataFrame（如果有多个数据源）
        # 使用 outer join 确保所有数据都被包含
        result = all_data[0]
        for df in all_data[1:]:
            result = pd.merge(
                result,
                df,
                on=['date', 'symbol'],
                how='outer'
            )

        return result


    def get_custom_factor(self, factor_logger: logging.Logger, user_id, factor_name, start_date, end_date,
                          symbol_type: Optional[str] = 'stock'):
        """获取用户自定义因子数据

        这个函数就像一个"自定义因子计算器"，它会获取用户创建的因子代码，
        执行计算，并返回因子数据。如果因子已经计算并持久化，会直接读取。

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
        factor_data = reader.get_custom_factor(
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
            RuntimeError: 如果数据库连接失败

        Example:
            >>> import logging
            >>> logger = logging.getLogger("test")
            >>> reader = FactorReader(config)
            >>> data = reader.get_custom_factor(
            ...     factor_logger=logger,
            ...     user_id=123,
            ...     factor_name="my_factor",
            ...     start_date="20240101",
            ...     end_date="20241231"
            ... )
        """
        try:
            # 检查因子是否已经计算并持久化到数据库
            # 持久化表的命名规则：factor_{factor_name}_{user_id}
            collection_name = f"factor_{factor_name}_{user_id}"
            if collection_name in self.db_handler.mongo_client[self.config["MONGO_DB"]].list_collection_names():
                # 如果持久化表存在，直接查询已计算的数据
                query = {
                    "date": {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                }

                # 查询持久化的因子数据
                records = self.db_handler.mongo_find(
                    self.config["MONGO_DB"],
                    collection_name,
                    query
                )

                if records:
                    # 将查询结果转换为 DataFrame，并设置多级索引
                    df = pd.DataFrame(list(records))
                    df = df.set_index(['date', 'symbol'])
                    df = df.drop(columns=['_id'])
                    return df

                logger.warning(f"No data found in {collection_name} for the specified date range")
                return None

            # 如果因子未持久化，需要从 user_factors 表获取因子定义并计算
            start_time = time.time()
            query = {
                "user_id": str(user_id),
                "factor_name": factor_name,
            }
            # 从 user_factors 表获取因子定义
            records = self.db_handler.mongo_find(
                self.config["MONGO_DB"],
                "user_factors",
                query
            )
            logger.info(
                f"Query user_factors took {time.time() - start_time:.3f} seconds for {factor_name} start_date: {start_date} end_date: {end_date}")
            
            # 如果因子不存在，返回 None
            if len(records) == 0:
                logger.warning(f"No data found for the specified parameters")
                return None
            
            # 提取因子定义信息
            query = {}  # 重置查询，用于后续股票过滤
            code_type = records[0]["code_type"]  # 因子类型：formula 或 python
            code = records[0]["code"]  # 因子代码
            st = records[0]["params"].get('include_st', True)  # 是否包含ST股票
            indicator = records[0]["params"].get('stock_pool', "000985")  # 股票池代码
            
            # 根据股票池代码构建查询条件
            if indicator != "000985":  # "000985" 表示全A股，不需要过滤
                if indicator == "000300":  # 沪深300
                    query["index_component"] = "100"
                elif indicator == "000905":  # 中证500
                    query["index_component"] = "010"
                elif indicator == "000852":  # 中证1000
                    query["index_component"] = "001"
            
            # ST股票过滤
            if not st:
                query["name"] = {"$not": {"$regex": "ST"}}
            
            # 根据查询条件获取符合条件的股票代码
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stock_market"
            )
            symbols = collection.distinct("symbol", query)

            if not symbols:
                logger.warning("No valid symbols found matching the criteria")
                return None

            # 延迟导入 MacroFactor 以避免循环依赖
            from panda_factor.generate.macro_factor import MacroFactor
            mf = MacroFactor()

            result = None
            try:
                # 根据因子类型执行不同的计算逻辑
                if code_type == "formula":
                    # 公式因子：使用公式表达式计算
                    result = mf.create_factor_from_formula(factor_logger, code, start_date, end_date, symbols,
                                                           symbol_type=symbol_type)
                elif code_type == "python":
                    # Python类因子：使用 Python 类计算
                    result = mf.create_factor_from_class(factor_logger, code, start_date, end_date, symbols,
                                                         symbol_type=symbol_type)
                else:
                    logger.warning(f"Unknown code type: {code_type}")
                    return None

                # 如果计算成功，将结果列名改为因子名称
                if result is not None:
                    result = result.rename(columns={"value": factor_name})
                return result

            except Exception as e:
                # 如果计算出错，记录详细的错误信息
                if code_type == "formula":
                    self._print_formula_error(e, code, factor_logger)
                else:
                    self._print_class_error(e, code, factor_logger)
                return None

        except Exception as e:
            # 如果因子设置出错，记录错误信息
            factor_logger.error("\n=== Factor Setup Error ===")
            factor_logger.error(f"Error in factor setup: {str(e)}")
            factor_logger.error(f"Error type: {type(e)}")
            return None

    def get_custom_factor_competition(self, factor_logger: logging.Logger, user_id, factor_id, start_date, end_date):
        """获取竞赛因子数据

        这个函数类似于 `get_custom_factor()`，但专门用于获取竞赛提交的因子数据。
        它从 `user_factor_submissions` 表读取因子定义，而不是从 `user_factors` 表。

        为什么需要这个函数？
        --------------------

        在因子竞赛中，用户提交的因子可能存储在专门的表中（`user_factor_submissions`），
        而不是普通的用户因子表。这个函数提供了获取竞赛因子数据的能力。

        工作原理
        --------

        与 `get_custom_factor()` 类似，但数据来源不同：
        1. 从 `user_factor_submissions` 表获取因子定义
        2. 提取因子代码和参数
        3. 执行计算并返回结果

        Args:
            factor_logger: 日志记录器，用于记录因子计算过程中的日志
            user_id: 用户 ID
            factor_id: 因子 ID（竞赛中的因子标识）
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD

        Returns:
            Optional[pd.DataFrame]: 因子数据 DataFrame，如果因子不存在或计算失败，返回 None

        Example:
            >>> import logging
            >>> logger = logging.getLogger("test")
            >>> reader = FactorReader(config)
            >>> data = reader.get_custom_factor_competition(
            ...     factor_logger=logger,
            ...     user_id=123,
            ...     factor_id=456,
            ...     start_date="20240101",
            ...     end_date="20241231"
            ... )
        """
        try:

            start_time = time.time()
            query = {
                "userId": int(user_id),
                "factorId": factor_id,
            }
            # Get data from MongoDB
            records = self.db_handler.mongo_find(
                self.config["MONGO_DB"],
                "user_factor_submissions",
                query
            )
            logger.info(
                f"Query user_factors took {time.time() - start_time:.3f} seconds for {factor_id} start_date: {start_date} end_date: {end_date}")
            if len(records) == 0:
                logger.warning(f"No data found for the specified parameters")
                return None
            query = {}
            factor_name = records[0]["factorDetails"]["factor_name"]
            code_type = records[0]["factorDetails"]["code_type"]
            code = records[0]["factorDetails"]["code"]
            st = records[0]["factorDetails"]["params"]['include_st']
            indicator = records[0]["factorDetails"]["params"]['stock_pool']
            if indicator != "000985":
                if indicator == "000300":
                    query["index_component"] = "100"
                elif indicator == "000905":
                    query["index_component"] = "010"
                elif indicator == "000852":
                    query["index_component"] = "001"
            if not st:
                query["name"] = {"$not": {"$regex": "ST"}}
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stock_market"
            )
            symbols = collection.distinct("symbol", query)

            if not symbols:
                logger.warning("No valid symbols found matching the criteria")
                return None

            # Lazy import MacroFactor to avoid circular dependency
            from panda_factor.generate.macro_factor import MacroFactor
            mf = MacroFactor()

            result = None
            try:
                if code_type == "formula":
                    result = mf.create_factor_from_formula(factor_logger, code, start_date, end_date, symbols)
                elif code_type == "python":
                    result = mf.create_factor_from_class(factor_logger, code, start_date, end_date, symbols)
                else:
                    logger.warning(f"Unknown code type: {code_type}")
                    return None

                if result is not None:
                    result = result.rename(columns={"value": factor_name})
                return result

            except Exception as e:
                if code_type == "formula":
                    self._print_formula_error(e, code, factor_logger)
                else:
                    self._print_class_error(e, code, factor_logger)
                return None

        except Exception as e:
            factor_logger.error("\n=== Factor Setup Error ===")
            factor_logger.error(f"Error in factor setup: {str(e)}")
            factor_logger.error(f"Error type: {type(e)}")
            return None

    def get_factor_by_name(self, factor_name, start_date, end_date):
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

        1. 根据因子名称查找因子定义（从 `user_factors` 表）
        2. 提取因子代码和参数
        3. 根据股票池和ST过滤条件获取股票代码
        4. 执行因子代码计算因子值
        5. 返回计算结果

        Args:
            factor_name: 因子名称
            start_date: 开始日期，格式 YYYYMMDD
            end_date: 结束日期，格式 YYYYMMDD

        Returns:
            Optional[pd.DataFrame]: 因子数据 DataFrame，如果因子不存在或计算失败返回 None

        Example:
            >>> reader = FactorReader(config)
            >>> data = reader.get_factor_by_name(
            ...     "momentum_factor",
            ...     "20240101",
            ...     "20241231"
            ... )
        """
        try:
            start_time = time.time()
            query = {
                "factor_name": factor_name,
            }
            # Get data from MongoDB
            records = self.db_handler.mongo_find(
                self.config["MONGO_DB"],
                "user_factors",
                query
            )
            logger.debug(
                f"Query user_factors took {time.time() - start_time:.3f} seconds for {factor_name} start_date: {start_date} end_date: {end_date}")
            if len(records) == 0:
                logger.warning(f"No data found for the specified parameters")
                return None
            query = {}
            code_type = records[0]["code_type"]
            code = records[0]["code"]

            # start_date = records[0]["params"]["start_date"]
            # end_date = records[0]["params"]["end_date"]
            st = records[0]["params"]['include_st']
            indicator = records[0]["params"]['stock_pool']
            if indicator != "000985":
                if indicator == "000300":
                    query["index_component"] = "100"
                elif indicator == "000905":
                    query["index_component"] = "010"
                elif indicator == "000852":
                    query["index_component"] = "001"
            if not st:
                query["name"] = {"$not": {"$regex": "ST"}}
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stock_market"
            )
            symbols = collection.distinct("symbol", query)

            if not symbols:
                logger.warning("No valid symbols found matching the criteria")
                return None

            # Lazy import MacroFactor to avoid circular dependency
            from panda_factor.generate.macro_factor import MacroFactor
            mf = MacroFactor()

            result = None
            try:
                if code_type == "formula":
                    result = mf.create_factor_from_formula(logger, code, start_date, end_date, symbols)
                elif code_type == "python":
                    result = mf.create_factor_from_class(logger, code, start_date, end_date, symbols)
                else:
                    logger.warning(f"Unknown code type: {code_type}")
                    return None

                if result is not None:
                    result = result.rename(columns={"value": factor_name})
                return result

            except Exception as e:
                if code_type == "formula":
                    self._print_formula_error(e, code, logger)
                else:
                    self._print_class_error(e, code, logger)
                return None

        except Exception as e:
            logger.error("\n=== Factor Setup Error ===")
            logger.error(f"Error in factor setup: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            return None

    def get_all_symbols(self):
        """获取所有股票代码

        这个函数就像一个"股票代码目录"，它会从数据库中获取所有唯一的股票代码。

        为什么需要这个函数？
        --------------------

        在查询因子数据时，如果用户没有指定股票代码，需要返回所有股票的数据。
        这个函数提供了快速获取所有股票代码的能力。

        工作原理
        --------

        1. 获取 stock_market 集合
        2. 使用 MongoDB 的 distinct 命令获取所有唯一的 symbol 值
        3. 返回股票代码列表

        性能说明
        --------

        - 使用 MongoDB 的 distinct 命令，性能很好
        - 结果会缓存在 `self.all_symbols` 中，避免重复查询

        Returns:
            List[str]: 所有股票代码的列表，如 ["000001", "000002", ...]

        Raises:
            RuntimeError: 如果数据库连接失败

        Example:
            >>> reader = FactorReader(config)
            >>> symbols = reader.get_all_symbols()
            >>> print(f"共有 {len(symbols)} 个股票代码")
        """
        # 获取股票市场数据集合
        collection = self.db_handler.get_mongo_collection(
            self.config["MONGO_DB"],
            "stock_market"
        )
        # 使用 distinct 命令获取所有唯一的股票代码
        # distinct 是 MongoDB 的高效命令，专门用于获取唯一值
        return collection.distinct("symbol")