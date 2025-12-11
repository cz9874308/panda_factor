"""
RiceQuant 因子数据清洗服务模块

本模块提供了从 RiceQuant 数据源获取和清洗因子数据的服务。
它会从 RiceQuant 获取因子的历史数据，进行清洗和转换，然后存储到 MongoDB。

核心概念
--------

- **因子数据**：从 RiceQuant 获取的各种因子数据（如财务因子、技术因子等）
- **数据清洗**：获取原始数据，清洗、转换格式，存储到数据库
- **并行处理**：使用多线程并行处理多个交易日的数据

为什么需要这个模块？
-------------------

在量化分析中，需要获取高质量的因子数据：
- RiceQuant 提供了丰富的因子数据
- 需要将数据清洗并统一格式
- 需要处理大量数据，并行处理可以提高效率

这个模块提供了从 RiceQuant 获取和清洗因子数据的能力。

工作原理（简单理解）
------------------

就像数据加工厂：

1. **连接数据源**：初始化 RiceQuant 连接
2. **获取原始数据**：从 RiceQuant 获取因子历史数据
3. **清洗数据**：清洗、转换数据格式
4. **存储数据**：将清洗后的数据存储到 MongoDB

注意事项
--------

- 需要 RiceQuant 的账号和密码
- 数据清洗会处理所有日期（包括非交易日）
- 使用并行处理提高效率，但要注意 API 调用频率限制
"""

from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from pymongo import UpdateOne
from tqdm import tqdm

import pandas as pd
import rqdatac
import traceback
from datetime import datetime
import time
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.rq_utils import get_ricequant_suffix


class FactorCleanerProService(ABC):
    """RiceQuant 因子数据清洗服务

    这个类提供了从 RiceQuant 数据源获取和清洗因子数据的功能。
    就像一个"因子数据加工厂"，它会从 RiceQuant 获取因子数据，清洗后存储到数据库。

    为什么需要这个类？
    -----------------

    在量化分析中，需要获取高质量的因子数据：
    - RiceQuant 提供了丰富的因子数据
    - 需要将数据清洗并统一格式
    - 需要处理大量数据，并行处理可以提高效率

    这个类提供了从 RiceQuant 获取和清洗因子数据的能力。

    工作原理（简单理解）
    ------------------

    就像数据加工厂：

    1. **连接数据源**：初始化 RiceQuant 连接
    2. **获取原始数据**：从 RiceQuant 获取因子历史数据
    3. **清洗数据**：清洗、转换数据格式
    4. **存储数据**：将清洗后的数据存储到 MongoDB

    实际使用场景
    -----------

    清洗一年的因子数据：

    ```python
    service = FactorCleanerProService(config)
    service.clean_history_data("2024-01-01", "2024-12-31")
    ```

    注意事项
    --------

    - 需要 RiceQuant 的账号和密码（在 config 中配置）
    - 数据清洗会处理所有日期（包括非交易日）
    - 使用并行处理提高效率，但要注意 API 调用频率限制
    """

    def __init__(self, config):
        """初始化因子数据清洗服务

        这个函数就像"启动因子数据加工厂"，它会：
        - 保存配置信息
        - 创建数据库连接
        - 初始化 RiceQuant 连接

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
            >>> service = FactorCleanerProService(config)
        """
        self.config = config
        # 数据库处理器，用于存储清洗后的数据
        self.db_handler = DatabaseHandler(config)
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

    def clean_history_data(self, start_date, end_date):
        """清洗历史因子数据

        这个函数就像一个"因子数据清洗流水线"，它会：
        - 生成日期范围
        - 并行处理多个交易日的数据
        - 清洗和存储因子数据

        为什么需要这个函数？
        --------------------

        在量化分析中，需要获取大量的历史因子数据：
        - 需要从外部数据源获取数据
        - 需要清洗和转换数据格式
        - 需要高效处理大量数据

        这个函数提供了完整的数据清洗流程。

        工作原理
        --------

        1. 生成日期范围（包括所有日期，不仅仅是交易日）
        2. 将日期分成多个批次
        3. 使用线程池并行处理每个批次
        4. 清洗每个日期的数据并存储

        Args:
            start_date: 开始日期，格式 YYYY-MM-DD
            end_date: 结束日期，格式 YYYY-MM-DD

        Returns:
            None: 函数不返回值，数据直接存储到数据库

        Example:
            >>> service = FactorCleanerProService(config)
            >>> service.clean_history_data("2024-01-01", "2024-12-31")
        """
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        trading_days = []
        for date in date_range:
            date_str = datetime.strftime(date, "%Y-%m-%d")
            trading_days.append(date_str)
        total_days = len(trading_days)
        processed_days = 0
        with tqdm(total=len(trading_days), desc="Processing Trading Days") as pbar:
            # 分批处理，每批5天
            batch_size = 5
            for i in range(0, len(trading_days), batch_size):
                batch_days = trading_days[i:i + batch_size]
                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = []
                    for date in batch_days:
                        futures.append(
                            executor.submit(
                                self.clean_daily_data,
                                date_str=date,
                                pbar=pbar
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
                        f"完成批次 {i // batch_size + 1}/{(len(trading_days) - 1) // batch_size + 1}，等待10秒后继续...")
                    time.sleep(10)
        logger.info("因子数据清洗全部完成！！！")

    def clean_daily_data(self, date_str, pbar):
        """补全当日数据"""
        try:
            date = date_str.replace('-', '')
            query = {"date": date}
            records = self.db_handler.mongo_find(self.config["MONGO_DB"], 'stock_market', query)
            if records is None or len(records) == 0:
                logger.info(f"records none for {date}")
                return

            data = pd.DataFrame(list(records))
            data = data[['date', 'symbol', 'open','high','low','close','volume']]
            data['order_book_id'] = data['symbol'].apply(get_ricequant_suffix)
            order_book_id_list = data['order_book_id'].tolist()
            logger.info("正在获取市值数据......")
            market_cap_data = rqdatac.get_factor(order_book_ids=order_book_id_list, factor=['market_cap'], start_date=date,
                                          end_date=date)
            merge_1 = data.merge(market_cap_data['market_cap'], on='order_book_id', how='left')
            logger.info("正在获取成交额数据......")
            price_data = rqdatac.get_price(order_book_ids=order_book_id_list, start_date=date, end_date=date,
                                           adjust_type='none')
            merge_2 = merge_1.merge(price_data['total_turnover'], on='order_book_id', how='left')
            logger.info("正在获取换手率数据......")
            turnover_data = rqdatac.get_turnover_rate(
                order_book_ids=order_book_id_list,
                start_date=date,
                end_date=date,
                fields=['today']
            )
            result_data = merge_2.merge(turnover_data['today'], on='order_book_id', how='left')
            result_data = result_data.drop(columns=['order_book_id'])
            result_data = result_data.rename(columns={'today': 'turnover'})
            result_data['market_cap'] = result_data['market_cap'].fillna(0)
            result_data['turnover'] = result_data['turnover'].fillna(0)
            result_data = result_data.rename(columns={'total_turnover': 'amount'})
            desired_order = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'market_cap', 'turnover','amount']
            result_data = result_data[desired_order]
            ensure_collection_and_indexes(table_name='factor_base')
            upsert_operations = []
            for record in result_data.to_dict('records'):
                upsert_operations.append(UpdateOne(
                    {'date': record['date'], 'symbol': record['symbol']},
                    {'$set': record},
                    upsert=True
                ))
            if upsert_operations:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['factor_base'].bulk_write(
                    upsert_operations)
                logger.info(f"Successfully upserted factor data for date: {date}")


        except Exception as e:
            error_msg = f"Failed to process market data for quarter : {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise


















