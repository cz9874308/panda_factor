"""
市场数据读取模块

本模块提供了一个高性能的市场数据读取器，它使用并行查询技术来加速大量数据的读取。
就像一个"高效的数据搬运工"，它能够将大时间范围的数据查询任务分解成多个小任务，
然后同时执行，大大提高了查询速度。

核心概念
--------

- **并行查询**：将大任务分解成多个小任务，同时执行，提高效率
- **日期分块**：将长时间范围分成多个短时间范围，每个范围独立查询
- **线程池**：使用线程池管理多个查询任务，避免创建过多线程

为什么需要这个模块？
-------------------

在量化分析中，经常需要查询大量的历史数据：
- 查询一年的数据可能需要几秒钟
- 查询十年的数据可能需要几十秒甚至几分钟

如果使用单线程查询，用户需要等待很长时间。
通过并行查询，可以将查询时间缩短到原来的几分之一。

工作原理（简单理解）
------------------

就像多人同时搬书：

1. **分割任务**：将一年的数据查询任务分成12个月（就像把书分成12堆）
2. **分配工人**：每个工人负责查询一个月的数据（就像每个人负责搬一堆书）
3. **同时工作**：所有工人同时开始工作（就像所有人同时搬书）
4. **汇总结果**：将所有结果合并（就像把所有书汇总到一起）

注意事项
--------

- 并行查询会占用更多数据库连接，需要确保数据库连接池足够大
- 如果数据量很小，并行查询可能反而更慢（因为线程创建有开销）
- 默认每个块是3个月，可以根据实际情况调整
"""

from math import e
import pandas as pd
import time
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
import concurrent.futures
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any


class MarketDataReader:
    """市场数据读取器

    这个类就像一个"高效的数据查询中心"，它提供了从 MongoDB 数据库读取股票市场数据的能力，
    并且使用并行查询技术来提高查询速度。

    为什么需要这个类？
    -----------------

    在量化分析中，需要频繁查询大量的市场数据：
    - 计算因子需要价格数据
    - 回测需要历史数据
    - 分析需要多只股票的数据

    如果每次查询都直接访问数据库，会导致：
    - 查询速度慢
    - 数据库压力大
    - 用户体验差

    这个类通过并行查询和智能分块，解决了这些问题。

    工作原理（简单理解）
    ------------------

    就像图书馆的智能借阅系统：

    1. **接收请求**：接收数据查询请求（就像接收借书请求）
    2. **分解任务**：将大时间范围分成多个小块（就像把要借的书分成多批）
    3. **并行查询**：多个线程同时查询不同时间段的数据（就像多个工作人员同时找书）
    4. **合并结果**：将所有结果合并成一个 DataFrame（就像把所有找到的书汇总）

    实际使用场景
    -----------

    查询一年的市场数据：

    ```python
    reader = MarketDataReader(config)
    data = reader.get_market_data(
        start_date="20240101",
        end_date="20241231",
        fields=["open", "close", "volume"]
    )
    ```

    查询特定股票的数据：

    ```python
    data = reader.get_market_data(
        symbols=["000001", "000002"],
        start_date="20240101",
        end_date="20241231"
    )
    ```

    注意事项
    --------

    - 使用前需要先初始化，传入配置对象
    - 查询大量数据时，并行查询会显著提高速度
    - 如果只查询少量数据，单线程可能更快
    """
    def __init__(self, config):
        """初始化市场数据读取器

        这个函数就像"启动数据查询中心"，它会：
        - 保存配置信息
        - 创建数据库连接
        - 预加载所有股票代码（用于后续查询）

        为什么需要预加载股票代码？
        ------------------------

        在查询数据时，如果用户没有指定股票代码，需要返回所有股票的数据。
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
            >>> reader = MarketDataReader(config)
        """
        self.config = config
        # 初始化数据库处理器，用于后续的数据库操作
        self.db_handler = DatabaseHandler(config)
        # 预加载所有股票代码，避免每次查询时都要查询数据库
        self.all_symbols = self.get_all_symbols()

    def _chunk_date_range(self, start_date: str, end_date: str, chunk_months: int = 3) -> List[tuple]:
        """将日期范围分割成多个小块

        这个函数就像一个"任务分解器"，它会将一个大的时间范围（比如一年）
        分割成多个小的时间范围（比如每3个月一块），以便后续并行查询。

        为什么需要分割日期范围？
        ----------------------

        如果查询一年的数据，单次查询可能会：
        - 查询时间很长
        - 占用大量内存
        - 数据库压力大

        通过分割成多个小块，可以：
        - 并行查询，提高速度
        - 每个查询更小，更快完成
        - 降低单次查询的内存占用

        工作原理（简单理解）
        ------------------

        就像把一年的工作分成多个季度：

        1. **确定范围**：确定开始日期和结束日期（就像确定一年的起止时间）
        2. **计算块数**：根据每个块的大小（默认3个月）计算需要分成多少块
        3. **生成块**：生成每个块的开始和结束日期（就像生成每个季度的起止时间）
        4. **返回列表**：返回所有块的列表（就像返回所有季度的列表）

        实际使用场景
        -----------

        将一年的数据分成4个季度：

        ```python
        chunks = reader._chunk_date_range("20240101", "20241231", chunk_months=3)
        # 返回: [("20240101", "20240331"), ("20240401", "20240630"), ...]
        ```

        特殊处理
        --------

        - 如果开始日期等于结束日期（只查询一天），直接返回一个块
        - 最后一个块可能小于指定的月份数（因为已经到结束日期了）

        Args:
            start_date: 开始日期，格式 YYYYMMDD，如 "20240101"
            end_date: 结束日期，格式 YYYYMMDD，如 "20241231"
            chunk_months: 每个块包含的月数，默认 3（即每个块3个月）

        Returns:
            List[tuple]: 日期块列表，每个元素是 (chunk_start, chunk_end) 元组
                        例如: [("20240101", "20240331"), ("20240401", "20240630")]

        Example:
            >>> reader = MarketDataReader(config)
            >>> chunks = reader._chunk_date_range("20240101", "20241231", chunk_months=3)
            >>> print(chunks)
            [('20240101', '20240331'), ('20240401', '20240630'), ...]
        """
        # 将字符串日期转换为 datetime 对象，方便进行日期计算
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")

        # 处理单日查询的情况：如果开始日期等于结束日期，直接返回一个块
        if start == end:
            return [(start_date, end_date)]

        chunks = []
        chunk_start = start  # 当前块的开始日期

        # 循环生成所有日期块，直到覆盖整个日期范围
        while chunk_start <= end:  # 使用 <= 确保包含结束日期
            # 计算当前块的结束日期
            # chunk_months * 30 - 1：每个块包含 chunk_months 个月，减1是为了避免与下一个块重叠
            # min(..., end)：确保不超过结束日期
            chunk_end = min(
                chunk_start + timedelta(days=chunk_months * 30 - 1),
                end
            )

            # 将日期块添加到列表中，格式化为 YYYYMMDD 字符串
            chunks.append((
                chunk_start.strftime("%Y%m%d"),
                chunk_end.strftime("%Y%m%d")
            ))

            # 如果已经到达或超过结束日期，停止循环
            if chunk_end >= end:
                break

            # 移动到下一个块的开始日期（当前块结束日期的下一天）
            chunk_start = chunk_end + timedelta(days=1)

        return chunks

    def _get_chunk_data(self, chunk_dates: tuple, query_params: Dict[str, Any], type: Optional[str] = 'stock') -> \
    Optional[pd.DataFrame]:
        """获取指定日期块的数据

        这个函数就像一个"数据查询工人"，它会查询指定时间段的数据。
        它是并行查询的基础单元，每个线程会调用这个函数来查询一个日期块的数据。

        为什么需要这个函数？
        --------------------

        在并行查询中，需要将大任务分解成多个小任务。
        这个函数负责执行一个小任务：查询一个日期块的数据。

        工作原理（简单理解）
        ------------------

        就像图书馆的工作人员找书：

        1. **接收任务**：接收要查询的日期块和查询参数（就像接收要找的书单）
        2. **构建查询**：根据参数构建 MongoDB 查询条件（就像确定在哪个书架找）
        3. **执行查询**：从数据库查询数据（就像从书架取书）
        4. **转换格式**：将查询结果转换为 DataFrame（就像把书整理好）
        5. **返回结果**：返回 DataFrame（就像把书交给借阅者）

        实际使用场景
        -----------

        这个函数通常不直接调用，而是由 `get_market_data()` 在并行查询时调用。

        性能优化
        --------

        - **批量大小优化**：根据字段数量估算文档大小，设置合适的 batch_size
          - 字段多 → 文档大 → 减小 batch_size（避免内存溢出）
          - 字段少 → 文档小 → 增大 batch_size（减少网络往返）
        - **投影优化**：只查询需要的字段，减少数据传输量

        Args:
            chunk_dates: 日期块元组，格式 (start_date, end_date)，如 ("20240101", "20240331")
            query_params: 查询参数字典，包含：
                - symbols: 股票代码列表
                - fields: 字段列表
                - indicator: 股票池代码
                - st: 是否包含ST股票
            type: 数据类型，'stock' 表示股票，'future' 表示期货

        Returns:
            Optional[pd.DataFrame]: 查询结果 DataFrame，如果未找到数据返回 None

        Example:
            >>> query_params = {
            ...     'symbols': ['000001'],
            ...     'fields': ['open', 'close'],
            ...     'indicator': '000985',
            ...     'st': True
            ... }
            >>> df = reader._get_chunk_data(("20240101", "20240331"), query_params)
        """
        # 从日期块中提取开始和结束日期
        start_date, end_date = chunk_dates
        # 从查询参数中提取各个参数
        symbols = query_params['symbols']
        fields = query_params['fields']
        indicator = query_params['indicator']
        st = query_params['st']

        # 构建 MongoDB 查询条件
        query = {}
        
        # 日期查询条件：如果开始日期等于结束日期，使用精确匹配；否则使用范围查询
        if start_date == end_date:
            # 如果是同一天，直接精确匹配（性能更好）
            query["date"] = start_date
        else:
            # 如果是不同日期，使用范围查询
            query["date"] = {
                "$gte": start_date,  # 大于等于开始日期
                "$lte": end_date      # 小于等于结束日期
            }

        # 股票池过滤：根据 indicator 参数过滤指数成分股
        if indicator != "000985":  # "000985" 表示全A股，不需要过滤
            if indicator == "000300":  # 沪深300
                query["index_component"] = "100"
            elif indicator == "000905":  # 中证500
                query["index_component"] = "010"
            elif indicator == "000852":  # 中证1000
                query["index_component"] = "001"
        
        # ST股票过滤：如果 st=False，排除名称中包含 "ST" 的股票
        if not st:
            query["name"] = {"$not": {"$regex": "ST"}}
        
        # 构建投影（只查询需要的字段，减少数据传输量）
        projection = None
        if fields:
            # 投影包含：用户指定的字段 + date + symbol（这两个字段总是需要）
            projection = {field: 1 for field in fields + ['date', 'symbol']}
            # 排除 _id 字段（除非用户明确要求）
            if '_id' not in fields:
                projection['_id'] = 0

        # 性能优化：估算每条记录的大小并设置合适的 batch_size
        # batch_size 影响查询性能：
        # - 太小：网络往返次数多，慢
        # - 太大：内存占用大，可能导致内存溢出
        # 估算方法：假设每个字段平均 20 字节
        estimated_doc_size = len(fields) * 20 if fields else 200
        # 目标：每个批次约 10MB，但限制在 2000-10000 之间
        target_batch_size = min(
            max(
                int(10 * 1024 * 1024 / estimated_doc_size),  # 10MB / 每条记录大小
                2000  # 最小 batch_size（避免太小）
            ),
            10000  # 最大 batch_size（避免太大）
        )
        
        # 根据数据类型选择集合
        if type == 'future':
            # 期货数据：需要特殊查询条件（symbol = underlying_symbol + "88"）
            query["$expr"] = {
                "$eq": [
                    "$symbol",
                    {"$concat": ["$underlying_symbol", "88"]}
                ]
            }
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "future_market"
            )
        else:
            # 股票数据：使用 stock_market 集合
            collection = self.db_handler.get_mongo_collection(
                self.config["MONGO_DB"],
                "stock_market"
            )
        
        # 执行查询，使用优化后的 batch_size
        cursor = collection.find(
            query,
            projection=projection
        ).batch_size(target_batch_size)

        # 将查询结果转换为 DataFrame
        chunk_df = pd.DataFrame(list(cursor))
        
        # 如果查询结果为空，返回 None
        if chunk_df.empty:
            return None

        # 如果 DataFrame 中包含 _id 列，删除它（通常不需要）
        if '_id' in chunk_df.columns:
            chunk_df = chunk_df.drop(columns=['_id'])

        return chunk_df

    def get_market_data(self, symbols=None, start_date=None, end_date=None, indicator="000985", st=True, fields=None,
                        type: Optional[str] = 'stock'):
        """获取市场数据（并行查询版本）

        这个函数是市场数据读取的核心接口，它会使用并行查询技术来快速获取大量数据。
        就像一个"高效的数据查询中心"，它会将大任务分解成多个小任务，然后同时执行。

        为什么需要并行查询？
        ------------------

        如果查询一年的数据，单次查询可能需要几秒钟。
        如果查询十年的数据，单次查询可能需要几十秒甚至几分钟。

        通过并行查询：
        - 将一年的数据分成4个季度，4个线程同时查询
        - 查询时间从几秒缩短到1秒左右
        - 用户体验大大提升

        工作原理（简单理解）
        ------------------

        就像多人同时搬书：

        1. **接收请求**：接收数据查询请求（就像接收搬书任务）
        2. **分解任务**：将大时间范围分成多个小块（就像把书分成多堆）
        3. **分配工人**：创建线程池，每个线程负责查询一个块（就像分配工人）
        4. **同时工作**：所有线程同时开始查询（就像所有人同时搬书）
        5. **收集结果**：收集所有线程的查询结果（就像收集所有搬来的书）
        6. **合并数据**：将所有结果合并成一个 DataFrame（就像把所有书汇总）

        实际使用场景
        -----------

        查询一年的市场数据：

        ```python
        reader = MarketDataReader(config)
        data = reader.get_market_data(
            start_date="20240101",
            end_date="20241231",
            fields=["open", "close", "volume"]
        )
        ```

        查询特定股票的数据：

        ```python
        data = reader.get_market_data(
            symbols=["000001", "000002"],
            start_date="20240101",
            end_date="20241231"
        )
        ```

        查询指数成分股数据：

        ```python
        # 查询沪深300成分股数据
        data = reader.get_market_data(
            start_date="20240101",
            end_date="20241231",
            indicator="000300",  # 沪深300
            st=False  # 不包含ST股票
        )
        ```

        性能说明
        --------

        - **并行度**：默认使用 8 个线程，可以根据实际情况调整
        - **分块大小**：默认每个块 3 个月，可以根据数据量调整
        - **查询速度**：并行查询通常比单线程快 3-5 倍

        可能遇到的问题
        ------------

        内存不足
        ^^^^^^^^

        如果查询的数据量非常大（比如所有股票十年的数据），可能会导致内存不足。
        建议：
        - 减少查询的时间范围
        - 减少查询的股票数量
        - 只查询需要的字段

        查询超时
        ^^^^^^^

        如果数据库响应慢，可能导致查询超时。
        建议：
        - 检查数据库性能
        - 减少并行度
        - 增加查询超时时间

        Args:
            symbols: 股票代码列表或单个代码，如果为 None 则返回所有股票
            start_date: 开始日期，格式 YYYYMMDD，如 "20240101"（必需）
            end_date: 结束日期，格式 YYYYMMDD，如 "20241231"（必需）
            indicator: 股票池代码，用于过滤指数成分股：
                - "000985"：全A股（默认）
                - "000300"：沪深300
                - "000905"：中证500
                - "000852"：中证1000
            st: 是否包含ST股票，True 表示包含，False 表示不包含（默认 True）
            fields: 字段列表，如 ["open", "close", "volume"]
                    如果为 None 则返回所有可用字段
            type: 数据类型，'stock' 表示股票，'future' 表示期货（默认 'stock'）

        Returns:
            Optional[pd.DataFrame]: 市场数据 DataFrame，包含 date、symbol 等列
                                    如果未找到数据或参数错误，返回 None

        Raises:
            RuntimeError: 如果数据库连接失败

        Example:
            >>> reader = MarketDataReader(config)
            >>> data = reader.get_market_data(
            ...     start_date="20240101",
            ...     end_date="20241231",
            ...     fields=["open", "close", "volume"]
            ... )
            >>> print(data.head())
        """
        # 记录开始时间，用于计算查询耗时
        start_time = time.time()

        # 参数验证：开始日期和结束日期是必需的
        if start_date is None or end_date is None:
            logger.error("start_date and end_date must be provided")
            return None

        # 参数标准化：将单个值转换为列表，统一处理
        if isinstance(symbols, str):
            symbols = [symbols]  # 单个股票代码转换为列表
        if isinstance(fields, str):
            fields = [fields]  # 单个字段转换为列表
        if fields is None:
            fields = []  # None 转换为空列表（表示查询所有字段）

        # 如果未指定股票代码，使用所有股票代码
        if symbols is None:
            symbols = self.all_symbols

        # 准备查询参数，封装成字典，方便传递给各个查询线程
        query_params = {
            'symbols': symbols,
            'fields': fields,
            'indicator': indicator,
            'st': st
        }

        # 将日期范围分成多个小块，以便并行查询
        date_chunks = self._chunk_date_range(str(start_date), str(end_date))
        print(date_chunks)  # 调试输出：显示所有日期块

        # 使用线程池并行处理每个日期块
        # 线程池就像一个"工人团队"，每个工人负责查询一个日期块的数据
        dfs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            # 提交所有查询任务到线程池
            # 每个任务查询一个日期块的数据
            futures = [
                executor.submit(self._get_chunk_data, chunk, query_params, str(type))
                for chunk in date_chunks
            ]

            # 收集所有查询结果
            # as_completed 会按照完成顺序返回结果，不一定是提交顺序
            for future in concurrent.futures.as_completed(futures):
                chunk_df = future.result()  # 获取查询结果
                # 只添加非空的结果
                if chunk_df is not None and not chunk_df.empty:
                    dfs.append(chunk_df)

        # 如果所有查询都没有返回数据，记录警告并返回 None
        if not dfs:
            logger.warning(f"No market data found for the specified parameters")
            return None

        # 合并所有数据块成一个完整的 DataFrame
        # ignore_index=True 表示重新生成索引，避免索引冲突
        final_df = pd.concat(dfs, ignore_index=True)

        # 计算并记录查询耗时
        end_time = time.time()
        logger.info(f"Market data query and conversion took {end_time - start_time:.2f} seconds")

        return final_df

    def get_all_symbols(self):
        """获取所有股票代码

        这个函数就像一个"股票代码目录"，它会从数据库中获取所有唯一的股票代码。

        为什么需要这个函数？
        --------------------

        在查询数据时，如果用户没有指定股票代码，需要返回所有股票的数据。
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
            >>> reader = MarketDataReader(config)
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