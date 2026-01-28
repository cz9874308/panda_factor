"""
XtQuant 因子数据清洗服务模块

本模块提供了从 XtQuant（迅投）数据源获取和清洗因子数据的服务。
它会从 XtQuant 获取因子的历史数据，进行清洗和转换，然后存储到 MongoDB。

核心概念
--------

- **因子数据**：从 XtQuant 获取的成交额等因子数据
- **数据清洗**：获取原始数据，清洗、转换格式，存储到数据库
- **并行处理**：使用多线程并行处理多个交易日的数据

为什么需要这个模块？
-------------------

在量化分析中，需要获取高质量的因子数据：
- XtQuant 提供了丰富的行情数据
- 需要将数据清洗并统一格式
- 需要处理大量数据，并行处理可以提高效率

工作原理（简单理解）
------------------

就像数据加工厂：

1. **连接数据源**：初始化 XtQuant 连接
2. **获取原始数据**：从 XtQuant 获取因子历史数据
3. **清洗数据**：清洗、转换数据格式
4. **存储数据**：将清洗后的数据存储到 MongoDB

注意事项
--------

- 需要 XtQuant 的认证信息（在 config 中配置）
- **重要**：迅投无法获取历史的市值和换手率，因此迅投数据源的 stock_market 表
  不包含 market_cap 和 turnover 这两个字段
- 使用并行处理提高效率
"""

import traceback
from abc import ABC
from concurrent.futures import ThreadPoolExecutor

from pymongo import UpdateOne
from tqdm import tqdm

from panda_common.handlers.database_handler import DatabaseHandler
# from xtquant import xtdata
# from xtquant import xtdatacenter as xtdc
from panda_common.logger_config import logger
import pandas as pd
from datetime import datetime

from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.xt_utils import get_xt_suffix, xt_get_amount, XTQuantManager


class FactorCleanerXTProService(ABC):
    """XtQuant 因子数据清洗服务

    这个类负责从 XtQuant 获取历史因子数据并进行清洗处理。

    Attributes:
        config: 配置字典，包含数据库连接信息
        db_handler: 数据库处理器实例
        progress_callback: 进度回调函数
    """
    def __init__(self,config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.progress_callback = None
        try:
            XTQuantManager.get_instance(config)
            logger.info("XtQuant ready to use")
        except Exception as e:
            error_msg = f"Failed to initialize XtQuant: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise
    def set_progress_callback(self, callback):
        '''进度条实现'''
        self.progress_callback = callback

    def factor_history_clean(self,start_date,end_date):
        logger.info("Starting XTData cleaning for XTQuant")

        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        trading_days = []
        for date in date_range:
            date_str = datetime.strftime(date, "%Y-%m-%d")
            trading_days.append(date_str)
        logger.info(f"找到 {len(trading_days)} 个交易日需要处理")
        total_days = len(trading_days)
        progress_days = 0
        with tqdm(total=total_days,desc = "Processing TradingDays") as pbar:
            batch_size = 5
            for i in range(0,total_days,batch_size):
                batch_days = trading_days[i:i+batch_size]
                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = []
                    for date in batch_days:
                        futures.append(
                            executor.submit(
                                self.factor_daily_clean,
                                date_str = date,
                                pbar = pbar,
                            ))
                    for future in futures:
                        try:
                            future.result()
                            progress_days += 1
                            progress = int((progress_days/total_days)*100)
                            # 更新进度
                            if self.progress_callback:
                                self.progress_callback(progress)
                            pbar.update(1)
                        except Exception as e:
                            logger.error(e)
                            pbar.update(1)
        logger.info("所有交易日数据处理完成")

    def factor_daily_clean(self,date_str,pbar):
        try:
            date = date_str.replace('-','')
            query = {"date":date}
            records = self.db_handler.mongo_find(self.config["MONGO_DB"],"stock_market",query)
            if records is None or len(records) == 0:
                logger.info(f"records none for {date}")
                return
            data = pd.DataFrame(list(records))
            data = data[['date','symbol','open','high','low','close','volume']]
            # 获取成交额数据
            logger.info("正在获取历史成交额数据.......")
            data['ts_code'] = data['symbol'].apply(get_xt_suffix)
            data['amount'] = data['ts_code'].apply(lambda code: xt_get_amount(code, date))
            data = data.drop(columns=['ts_code'])
            ensure_collection_and_indexes(table_name='factor_base')
            upsert_operations = []
            for record in data.to_dict('records'):
                upsert_operations.append(UpdateOne(
                    {'date':record['date'],'symbol':record['symbol']},
                    {'$set':record},
                    upsert=True
                ))
            if upsert_operations:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['factor_base'].bulk_write(upsert_operations)
                logger.info(f"Successfully upserted factor data for date:{date}")

        except Exception as e:
            error_msg = f"Failed to process factor for quanter: {e}"
            logger.error(error_msg)
            raise



