"""
Tushare 因子数据清洗服务模块

本模块提供了从 Tushare 数据源获取和清洗因子数据的服务。
它会从 Tushare 获取因子的历史数据，进行清洗和转换，然后存储到 MongoDB。

核心概念
--------

- **因子数据**：从 Tushare 获取的市值、换手率、成交额等因子数据
- **数据清洗**：获取原始数据，清洗、转换格式，存储到数据库
- **并行处理**：使用多线程并行处理多个交易日的数据

为什么需要这个模块？
-------------------

在量化分析中，需要获取高质量的因子数据：
- Tushare 提供了丰富的因子数据
- 需要将数据清洗并统一格式
- 需要处理大量数据，并行处理可以提高效率

工作原理（简单理解）
------------------

就像数据加工厂：

1. **连接数据源**：初始化 Tushare 连接
2. **获取原始数据**：从 Tushare 获取因子历史数据
3. **清洗数据**：清洗、转换数据格式
4. **存储数据**：将清洗后的数据存储到 MongoDB

注意事项
--------

- 需要 Tushare 的 Token（在 config 中配置）
- 使用并行处理提高效率，但要注意 API 调用频率限制
"""

from abc import ABC
import tushare as ts
from pymongo import UpdateOne
import traceback

import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import time
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.ts_utils import get_tushare_suffix


class FactorCleanerTSProService(ABC):
    """Tushare 因子数据清洗服务

    这个类负责从 Tushare 获取历史因子数据并进行清洗处理。

    Attributes:
        config: 配置字典，包含数据库连接和 Tushare Token
        db_handler: 数据库处理器实例
        progress_callback: 进度回调函数
        pro: Tushare Pro API 实例
    """

    def __init__(self,config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        self.progress_callback = None
        try:
            ts.set_token(config['TS_TOKEN'])
            self.pro = ts.pro_api()
        except Exception as e:
            error_msg = f"Failed to initialize tushare: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise

    def set_progress_callback(self, callback):
        self.progress_callback = callback

    def clean_history_data(self, start_date, end_date):
        """补全历史数据"""
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        trading_days = []
        for date in date_range:
            date_str = datetime.strftime(date, "%Y-%m-%d")
            trading_days.append(date_str)
        total_days = len(trading_days)
        processed_days = 0
        with tqdm(total=len(trading_days), desc="Processing Trading Days") as pbar:
            # 分批处理，每批10天
            batch_size = 10
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
        """补全当日数据(历史循环补充)"""
        try:
            date = date_str.replace('-', '')
            query = {"date": date}
            records = self.db_handler.mongo_find(self.config["MONGO_DB"], 'stock_market', query)
            if records is None or len(records) == 0:
                logger.info(f"records none for {date}")
                return

            data = pd.DataFrame(list(records))
            data = data[['date', 'symbol', 'open','high','low','close','volume']]
            data['ts_code'] = data['symbol'].apply(get_tushare_suffix)

            logger.info("正在获取市值和换手率数据......")
            factor_data = self.pro.query('daily_basic', trade_date=date,fields=['ts_code','turnover_rate','total_mv'])
            temp_data = data.merge(factor_data[['ts_code','turnover_rate','total_mv']], on='ts_code', how='left')
            temp_data = temp_data.rename(columns={'total_mv': 'market_cap'})
            temp_data = temp_data.rename(columns={'turnover_rate': 'turnover'})
            logger.info("正在获取成交额数据......")
            price_data = self.pro.query("daily", trade_date=date, fields=['ts_code', 'amount'])
            result_data = temp_data.merge(price_data[['ts_code', 'amount']], on='ts_code', how='left')
            result_data = result_data.drop(columns=['ts_code'])
            # tushare的成交额是以千元为单位的
            result_data['amount'] = result_data['amount']*1000
            result_data['market_cap'] = result_data['market_cap'] * 10000
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
