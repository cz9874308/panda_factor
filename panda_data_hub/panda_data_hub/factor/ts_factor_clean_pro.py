"""
Tushare 因子数据清洗模块

本模块提供了从 Tushare 数据源获取和清洗因子数据的清洗器类。
它负责获取市值、换手率、成交额等基础因子数据，并将数据清洗后存储到 MongoDB。

核心概念
--------

- **基础因子**：市值 (market_cap)、换手率 (turnover)、成交额 (amount) 等用于因子计算的基础数据
- **数据清洗**：从 Tushare 获取数据，与本地行情数据合并，存储到数据库

为什么需要这个模块？
-------------------

在因子计算中，需要一些基础因子数据：
- 市值数据：用于市值加权、市值中性化等
- 换手率数据：用于流动性相关的因子计算
- 成交额数据：用于资金流向相关的因子计算

工作原理（简单理解）
------------------

1. **读取本地行情**：从 MongoDB 获取当日行情数据
2. **获取因子数据**：从 Tushare 获取市值、换手率、成交额等数据
3. **数据合并**：将因子数据与行情数据合并
4. **存储数据**：将清洗后的数据存储到 MongoDB

注意事项
--------

- 需要有效的 Tushare Token
- 依赖本地已有的行情数据
"""

import traceback
from abc import ABC
from datetime import datetime

import pandas as pd
from pymongo import UpdateOne

from panda_common.handlers.database_handler import DatabaseHandler
import tushare as ts

from panda_common.logger_config import logger
from panda_data_hub.utils.mongo_utils import ensure_collection_and_indexes
from panda_data_hub.utils.ts_utils import get_tushare_suffix


class TSFactorCleaner(ABC):
    """Tushare 因子数据清洗器

    这个类负责从 Tushare 获取因子数据并进行清洗处理。

    Attributes:
        config: 配置字典，包含数据库连接和 Tushare Token
        db_handler: 数据库处理器实例
        pro: Tushare Pro API 实例
    """

    def __init__(self, config):
        self.config = config
        self.db_handler = DatabaseHandler(config)
        try:
            ts.set_token(config['TS_TOKEN'])
            self.pro = ts.pro_api()
        except Exception as e:
            error_msg = f"Failed to initialize tushare: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise

    def clean_daily_factor(self):
        try:
            date = datetime.now().strftime('%Y%m%d')
            query = {"date": date}
            records = self.db_handler.mongo_find(self.config["MONGO_DB"], 'stock_market', query)
            if records is None or len(records) == 0:
                logger.info(f"records none for {date}")
                return

            data = pd.DataFrame(list(records))
            data = data[['date', 'symbol', 'open','high','low','close','volume']]
            data['ts_code'] = data['symbol'].apply(get_tushare_suffix)

            logger.info("正在获取市值和换手率数据数据......")
            factor_data = self.pro.query('daily_basic', trade_date=date,fields=['ts_code','turnover_rate','total_mv'])
            temp_data = data.merge(factor_data[['ts_code','turnover_rate','total_mv']], on='ts_code', how='left')
            temp_data = temp_data.rename(columns={'total_mv': 'market_cap'})
            temp_data = temp_data.rename(columns={'turnover_rate': 'turnover'})
            logger.info("正在获取成交额数据......")
            price_data = self.pro.query("daily", trade_date=date, fields=['ts_code', 'amount'])
            result_data = temp_data.merge(price_data[['ts_code', 'amount']], on='ts_code', how='left')
            result_data = result_data.drop(columns=['ts_code'])
            # tushare的成交额是以千元为单位的
            result_data['amount'] = result_data['amount'] * 1000
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