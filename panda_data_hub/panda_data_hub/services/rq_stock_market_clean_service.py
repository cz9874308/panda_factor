"""
RiceQuant 股票市场数据清洗服务模块

本模块提供了从 RiceQuant（米筐）数据源获取和清洗股票市场数据的服务。
它会从 RiceQuant 获取股票的历史行情数据，进行清洗和转换，然后存储到 MongoDB。

核心概念
--------

- **数据源**：RiceQuant（米筐）数据平台
- **数据清洗**：获取原始数据，清洗、转换格式，存储到数据库
- **并行处理**：使用多线程并行处理多个交易日的数据

为什么需要这个模块？
-------------------

在量化分析中，需要获取高质量的股票市场数据：
- RiceQuant 提供了丰富的历史数据
- 需要将数据清洗并统一格式
- 需要处理大量数据，并行处理可以提高效率

这个模块提供了从 RiceQuant 获取和清洗数据的能力。

工作原理（简单理解）
------------------

就像数据加工厂：

1. **连接数据源**：初始化 RiceQuant 连接（就像连接供应商）
2. **获取原始数据**：从 RiceQuant 获取历史行情数据（就像获取原材料）
3. **清洗数据**：清洗、转换数据格式（就像加工原材料）
4. **存储数据**：将清洗后的数据存储到 MongoDB（就像入库）

注意事项
--------

- 需要 RiceQuant 的账号和密码
- 数据清洗会跳过非交易日
- 使用并行处理提高效率，但要注意 API 调用频率限制
"""

import time
from datetime import datetime

from pymongo import UpdateOne

from tqdm import tqdm
import pandas as pd
import traceback
from abc import ABC
import rqdatac
from concurrent.futures import ThreadPoolExecutor
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_common.utils.stock_utils import get_exchange_suffix
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.rq_utils import get_index_components, rq_is_trading_day


class StockMarketCleanRQServicePRO(ABC):
    """RiceQuant 股票市场数据清洗服务

    这个类提供了从 RiceQuant 数据源获取和清洗股票市场数据的功能。
    就像一个"数据加工厂"，它会从 RiceQuant 获取数据，清洗后存储到数据库。

    为什么需要这个类？
    -----------------

    在量化分析中，需要获取高质量的股票市场数据：
    - RiceQuant 提供了丰富的历史数据
    - 需要将数据清洗并统一格式
    - 需要处理大量数据，并行处理可以提高效率

    这个类提供了从 RiceQuant 获取和清洗数据的能力。

    工作原理（简单理解）
    ------------------

    就像数据加工厂：

    1. **连接数据源**：初始化 RiceQuant 连接（就像连接供应商）
    2. **获取原始数据**：从 RiceQuant 获取历史行情数据（就像获取原材料）
    3. **清洗数据**：清洗、转换数据格式（就像加工原材料）
    4. **存储数据**：将清洗后的数据存储到 MongoDB（就像入库）

    实际使用场景
    -----------

    清洗一年的股票市场数据：

    ```python
    service = StockMarketCleanRQServicePRO(config)
    service.stock_market_clean_by_time("2024-01-01", "2024-12-31")
    ```

    注意事项
    --------

    - 需要 RiceQuant 的账号和密码（在 config 中配置）
    - 数据清洗会跳过非交易日
    - 使用并行处理提高效率，但要注意 API 调用频率限制
    """

    def __init__(self, config):
        """初始化数据清洗服务

        这个函数就像"启动数据加工厂"，它会：
        - 保存配置信息
        - 创建数据库连接
        - 初始化 RiceQuant 连接

        为什么需要初始化 RiceQuant？
        ----------------------------

        RiceQuant 是一个数据平台，需要认证才能访问数据。
        初始化时会验证账号和密码，建立连接。

        工作原理
        --------

        1. **保存配置**：保存传入的配置对象
        2. **创建连接**：创建数据库处理器，建立数据库连接
        3. **初始化数据源**：使用账号密码初始化 RiceQuant 连接
        4. **初始化缓存**：初始化指数成分股缓存（后续会加载）

        Args:
            config: 配置字典，必须包含：
                - MUSER: RiceQuant 用户名
                - MPASSWORD: RiceQuant 密码
                - MongoDB 连接信息

        Raises:
            Exception: 如果 RiceQuant 初始化失败，会抛出异常

        Example:
            >>> from panda_common.config import get_config
            >>> config = get_config()
            >>> service = StockMarketCleanRQServicePRO(config)
        """
        self.config = config
        # 数据库处理器，用于存储清洗后的数据
        self.db_handler = DatabaseHandler(config)
        # 指数成分股缓存（后续会加载）
        self.hs300_components = None  # 沪深300成分股
        self.zz500_components = None  # 中证500成分股
        self.zz1000_components = None  # 中证1000成分股
        # 进度回调函数（用于更新进度）
        self.progress_callback = None
        
        try:
            # 初始化 RiceQuant 连接，需要用户名和密码
            rqdatac.init(config['MUSER'], config['MPASSWORD'])
            logger.info("RiceQuant initialized successfully")
            rqdatac.info()  # 打印 RiceQuant 信息
        except Exception as e:
            error_msg = f"Failed to initialize RiceQuant: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise

    def set_progress_callback(self, callback):
        """设置进度回调函数

        这个函数用于设置进度更新回调，可以在数据清洗过程中实时更新进度。

        Args:
            callback: 回调函数，接收一个整数参数（0-100），表示进度百分比

        Example:
            >>> def update_progress(progress):
            ...     print(f"进度: {progress}%")
            >>> service.set_progress_callback(update_progress)
        """
        self.progress_callback = callback

    def stock_market_clean_by_time(self, start_date, end_date):
        """按时间范围清洗股票市场数据

        这个函数就像一个"数据清洗流水线"，它会：
        - 从 RiceQuant 获取指定时间范围内的所有股票数据
        - 清洗和转换数据格式
        - 并行处理多个交易日的数据
        - 存储到 MongoDB

        为什么需要这个函数？
        --------------------

        在量化分析中，需要获取大量的历史市场数据：
        - 需要从外部数据源获取数据
        - 需要清洗和转换数据格式
        - 需要高效处理大量数据

        这个函数提供了完整的数据清洗流程。

        工作原理（简单理解）
        ------------------

        就像数据加工流水线：

        1. **获取原材料**：从 RiceQuant 获取所有股票的历史行情数据（就像获取原材料）
        2. **获取辅助信息**：获取股票名称变更信息和指数成分股信息（就像获取辅助材料）
        3. **筛选交易日**：筛选出交易日，跳过非交易日（就像筛选可用材料）
        4. **分批处理**：将交易日分成多个批次，并行处理（就像分批加工）
        5. **清洗存储**：清洗每个交易日的数据并存储（就像加工并入库）

        性能优化
        --------

        - **批量获取**：一次性获取所有日期的数据，避免重复请求
        - **并行处理**：使用线程池并行处理多个交易日，提高效率
        - **分批处理**：将交易日分成批次，避免同时创建过多线程

        Args:
            start_date: 开始日期，格式 YYYY-MM-DD，如 "2024-01-01"
            end_date: 结束日期，格式 YYYY-MM-DD，如 "2024-12-31"

        Returns:
            None: 函数不返回值，数据直接存储到数据库

        Raises:
            Exception: 如果数据获取或清洗失败，会抛出异常

        Example:
            >>> service = StockMarketCleanRQServicePRO(config)
            >>> service.stock_market_clean_by_time("2024-01-01", "2024-12-31")
        """
        logger.info("Starting market data cleaning for rqdatac")
        
        # 获取所有股票代码（CS表示股票，cn表示中国市场）
        symbol_list = rqdatac.all_instruments(type='CS', market='cn', date=None)
        symbol_list = symbol_list['order_book_id']
        
        # 获取所有日期股票的历史行情（一次性获取，避免重复请求）
        price_data = rqdatac.get_price(order_book_ids=symbol_list, start_date=start_date, end_date=end_date, adjust_type='none')
        
        # 获取所有日期的所有股票的历史名称变更信息（用于处理股票名称变更）
        symbol_change_info = rqdatac.get_symbol_change_info(symbol_list)
        
        # 获取所有日期的指数成分股票（用于标记指数成分股）
        self.hs300_components, self.zz500_components, self.zz1000_components = get_index_components(start_date, end_date)
        
        # 获取交易日：生成日期范围，筛选出交易日
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        trading_days = []
        for date in date_range:
            date_str = datetime.strftime(date, "%Y-%m-%d")
            if rq_is_trading_day(date_str):  # 检查是否为交易日
                trading_days.append(date_str)
            else:
                logger.info(f"跳过非交易日: {date_str}")
        logger.info(f"找到 {len(trading_days)} 个交易日需要处理")
        
        # 根据交易日循环处理
        total_days = len(trading_days)
        processed_days = 0
        with tqdm(total=len(trading_days), desc="Processing Trading Days") as pbar:
            # 分批处理，每批8天（避免同时创建过多线程）
            batch_size = 8
            for i in range(0, len(trading_days), batch_size):
                batch_days = trading_days[i:i + batch_size]
                # 使用线程池并行处理当前批次的所有交易日
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = []
                    for date in batch_days:
                        # 提交清洗任务到线程池
                        futures.append(executor.submit(
                            self.clean_meta_market_data,
                            price_data_origin=price_data,
                            symbol_change_info_origin=symbol_change_info,
                            date=date
                        ))

                    # 等待当前批次的所有任务完成
                    for future in futures:
                        try:
                            future.result()
                            processed_days += 1
                            progress = int((processed_days / total_days) * 100)

                            # 更新进度
                            if self.progress_callback:
                                self.progress_callback(progress)
                            pbar.update(1)
                        except Exception as e:
                            logger.error(f"Task failed: {e}")
                            pbar.update(1)  # 即使任务失败也更新进度条

                # 批次之间添加短暂延迟，避免连接数超限
                if i + batch_size < len(trading_days):
                    logger.info(
                        f"完成批次 {i // batch_size + 1}/{(len(trading_days) - 1) // batch_size + 1}，等待5秒后继续...")
                    time.sleep(5)

        logger.info("所有交易日数据处理完成")

    def clean_meta_market_data(self,price_data_origin, symbol_change_info_origin,date):
        try:
            # 为保证线程安全，特此加上副本
            price_data = price_data_origin.copy()
            symbol_change_info =symbol_change_info_origin.copy()
            # 重置股票行情数据索引
            price_data.reset_index(drop=False,inplace=True)
            symbol_change_info.reset_index(drop=False, inplace=True)
            # 获得指定日期的股票行情数据
            price_daily_data = price_data[price_data['date'] == date]

            # 洗 index_components列
            price_daily_data['index_component'] = None
            for idx, row in price_daily_data.iterrows():
                try:
                    component = self.clean_index_components(data_symbol=row['order_book_id'],date=date)
                    price_daily_data.at[idx, 'index_component'] = component
                except Exception as e:
                    logger.error(f"Failed to clean index for {row['order_book_id']} on {date}: {str(e)}")
                    continue

            # 洗name列
            price_daily_data['name'] = None
            for idx, row in price_daily_data.iterrows():
                try:
                    stock_name = self.clean_stock_name(symbol_change_info = symbol_change_info,data_symbol=row['order_book_id'], date=date)
                    price_daily_data.at[idx, 'name'] = stock_name
                except Exception as e:
                    logger.error(f"Failed to clean index for {row['order_book_id']} on {date}: {str(e)}")
                    continue

            # 洗其他列
            price_daily_data = price_daily_data.drop(columns=['num_trades', 'total_turnover'])
            price_daily_data['date'] = pd.to_datetime(price_daily_data['date']).dt.strftime("%Y%m%d")
            price_daily_data = price_daily_data.rename(columns={'order_book_id': 'symbol'})
            price_daily_data = price_daily_data.rename(columns={'prev_close': 'pre_close'})
            price_daily_data['symbol'] = price_daily_data['symbol'].apply(get_exchange_suffix)
            desired_order = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'pre_close',
                             'limit_up', 'limit_down', 'index_component', 'name']
            price_daily_data = price_daily_data[desired_order]
            # 检索数据库索引
            ensure_collection_and_indexes(table_name = 'stock_market')
            # 执行插入操作
            upsert_operations = []
            for record in price_daily_data.to_dict('records'):
                upsert_operations.append(UpdateOne(
                    {'date': record['date'], 'symbol': record['symbol']},
                    {'$set': record},
                    upsert=True
                ))
            if upsert_operations:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['stock_market'].bulk_write(
                    upsert_operations)
                logger.info(f"Successfully upserted market data for date: {date}")

        except Exception as e:
            logger.error({e})

    def clean_index_components(self,data_symbol,date):
        try:
            target_date = pd.to_datetime(date)
            # 初始化标记
            # index_mark = '000'
            if self.hs300_components and target_date in self.hs300_components and data_symbol in self.hs300_components[target_date]:
                index_mark = '100'
            elif  self.zz500_components and target_date in self.zz500_components and data_symbol in self.zz500_components[target_date]:
                index_mark = '010'
            elif self.zz1000_components and target_date in self.zz1000_components and data_symbol in self.zz1000_components[target_date]:
                index_mark = '001'
            else:
                index_mark = '000'
            return index_mark
        except Exception as e:
            logger.error(f"Error checking if {data_symbol} is in index components on {date}: {str(e)}")
            return None  # 或者返回其他默认值


    def clean_stock_name(self,symbol_change_info,data_symbol,date):
        try:
            daily_info = symbol_change_info[symbol_change_info['order_book_id'] == data_symbol]

            valid_changes = daily_info[daily_info['change_date'] <= date]
            if not valid_changes.empty:
                # 按开始日期排序，获取最新的变更记录
                # valid_changes.reset_index(inplace=True, drop=False)
                latest_change = valid_changes.sort_values('change_date', ascending=False).iloc[0]
                latest_symbol = latest_change['symbol']
                return latest_symbol
            else:
                stock_info = rqdatac.instruments(order_book_ids=data_symbol)
                current_name = stock_info.symbol
                return current_name

        except Exception as e:
            logger.error(f"Error checking if {data_symbol} is in index components on {date}: {str(e)}")
            return None  # 或者返回其他默认值