"""
迅投 (XtQuant) 因子数据清洗模块

本模块提供了从 XtQuant（迅投量化）数据源获取和清洗因子数据的清洗器类。
它负责获取成交量、成交额等基础因子数据，并将数据清洗后存储到 MongoDB。

核心概念
--------

- **基础因子**：成交量 (volume)、成交额 (amount) 等用于因子计算的基础数据
- **数据清洗**：从 XtQuant 获取数据，与本地行情数据合并，存储到数据库

为什么需要这个模块？
-------------------

在因子计算中，需要一些基础因子数据：
- 成交量数据：用于量价相关的因子计算
- 成交额数据：用于资金流向相关的因子计算

工作原理（简单理解）
------------------

1. **读取本地行情**：从 MongoDB 获取当日行情数据
2. **获取因子数据**：从 XtQuant 获取成交量、成交额等数据
3. **数据合并**：将因子数据与行情数据合并
4. **存储数据**：将清洗后的数据存储到 MongoDB

注意事项
--------

- 需要安装并配置 XtQuant 客户端
- 依赖本地已有的行情数据
"""

import traceback
from abc import ABC

import pandas as pd
from pymongo import UpdateOne

from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.logger_config import logger
from datetime import datetime

from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.xt_utils import xt_is_trading_day, get_xt_suffix, xt_get_total_volume, \
    xt_get_amount, XTQuantManager


class XTFactorCleaner(ABC):
    """迅投 (XtQuant) 因子数据清洗器

    这个类负责从 XtQuant 获取因子数据并进行清洗处理。

    Attributes:
        config: 配置字典，包含数据库连接信息
        db_handler: 数据库处理器实例
    """
    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        try:
            XTQuantManager.get_instance(config)
            logger.info("XtQuant ready to use")
        except Exception as e:
            error_msg = f"Failed to initialize XtQuant: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise

    def clean_daily_factor(self):
        date_str = datetime.now().strftime("%Y%m%d")
        if xt_is_trading_day(date_str):
            try:
                logger.info(f"开始清洗因子数据: {date_str}")
                self.clean_factor_data(date_str=date_str)
            except Exception as e:
                logger.error(f"{str(e)}")
                return 0
        else:
            logger.info(f"跳过非交易日: {date_str}")
            return

    def clean_factor_data(self, date_str):
        try:
            date = date_str.replace('-', '')
            query = {"date": date}
            records = self.db_handler.mongo_find(self.config["MONGO_DB"], "stock_market", query)
            if records is None or len(records) == 0:
                logger.info(f"records none for {date}")
                return
            data = pd.DataFrame(list(records))
            data = data[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            # 获取计算换手率和市值必要的数据
            data['stock_code'] = data['symbol'].apply(get_xt_suffix)
            data['TotalVolume'] = data['stock_code'].apply(xt_get_total_volume)
            # 计算换手率和市值因子
            data['turnover'] = data['volume'] / data['TotalVolume']
            data['turnover'] = data['turnover'] * 100
            data['turnover'] = data['turnover'].round(4)
            data['market_cap'] = data['close'] * data['TotalVolume']
            # 获取成交额数据
            logger.info("正在获取历史成交额数据.......")
            data['amount'] = data['stock_code'].apply(lambda code: xt_get_amount(code, date))
            data = data.drop(columns=['TotalVolume', 'stock_code'])
            ensure_collection_and_indexes(table_name='factor_base')
            upsert_operations = []
            for record in data.to_dict('records'):
                upsert_operations.append(UpdateOne(
                    {'date': record['date'], 'symbol': record['symbol']},
                    {'$set': record},
                    upsert=True
                ))
            if upsert_operations:
                self.db_handler.mongo_client[self.config["MONGO_DB"]]['factor_base'].bulk_write(upsert_operations)
                logger.info(f"Successfully upserted factor data for date:{date}")
        except Exception as e:
            error_msg = f"Failed to process factor for quanter: {e}"
            logger.error(error_msg)
            raise
