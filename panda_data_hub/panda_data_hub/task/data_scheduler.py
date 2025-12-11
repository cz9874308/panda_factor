"""
数据清洗任务调度模块

本模块提供了数据清洗任务的定时调度功能，使用 APScheduler 实现定时任务。
它会按照配置的时间自动执行数据清洗任务，保持数据的及时更新。

核心概念
--------

- **任务调度**：使用 APScheduler 实现定时任务调度
- **定时清洗**：按照配置的时间自动执行数据清洗
- **后台运行**：调度器在后台运行，不阻塞主程序

为什么需要这个模块？
-------------------

在量化分析中，需要定期更新数据：
- 每天需要清洗最新的市场数据
- 需要自动执行，无需人工干预
- 需要支持多个数据源

这个模块提供了定时任务调度的能力。

工作原理（简单理解）
------------------

就像定时闹钟：

1. **设置闹钟**：配置任务执行时间（就像设置闹钟时间）
2. **等待触发**：调度器等待到指定时间（就像等待闹钟响起）
3. **执行任务**：自动执行数据清洗任务（就像执行闹钟提醒的事情）
4. **循环执行**：每天重复执行（就像每天都会响的闹钟）

注意事项
--------

- 调度器在后台运行，不会阻塞主程序
- 任务执行时间由 config 中的 DATA_UPDATE_TIME 配置
- 支持多个数据源，根据 DATAHUBSOURCE 配置选择
"""

from panda_common.config import config, logger
from panda_common.handlers.database_handler import DatabaseHandler


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import datetime

from panda_data_hub.data.ricequant_stock_market_cleaner import RQStockMarketCleaner
from panda_data_hub.data.ricequant_stocks_cleaner import RQStockCleaner
from panda_data_hub.data.tushare_stocks_cleaner import TSStockCleaner
from panda_data_hub.data.tushare_stock_market_cleaner import TSStockMarketCleaner
# from panda_data_hub.data.xtquant_stock_market_cleaner import XTStockMarketCleaner
# from panda_data_hub.data.xtquant_stocks_cleaner import XTStockCleaner


class DataScheduler:
    """数据清洗任务调度器

    这个类就像一个"定时任务管理器"，它会按照配置的时间自动执行数据清洗任务。
    使用 APScheduler 实现定时调度，支持 Cron 表达式。

    为什么需要这个类？
    -----------------

    在量化分析中，需要定期更新数据：
    - 每天需要清洗最新的市场数据
    - 需要自动执行，无需人工干预
    - 需要支持多个数据源

    这个类提供了定时任务调度的能力。

    工作原理（简单理解）
    ------------------

    就像定时闹钟：

    1. **设置闹钟**：配置任务执行时间（就像设置闹钟时间）
    2. **等待触发**：调度器等待到指定时间（就像等待闹钟响起）
    3. **执行任务**：自动执行数据清洗任务（就像执行闹钟提醒的事情）
    4. **循环执行**：每天重复执行（就像每天都会响的闹钟）

    实际使用场景
    -----------

    每天下午3点自动清洗市场数据：

    ```python
    scheduler = DataScheduler()
    scheduler.schedule_data()  # 配置并启动定时任务
    ```

    注意事项
    --------

    - 调度器在后台运行，不会阻塞主程序
    - 任务执行时间由 config 中的 DATA_UPDATE_TIME 配置
    - 支持多个数据源，根据 DATAHUBSOURCE 配置选择
    """
    def __init__(self):
        """初始化数据调度器

        这个函数就像"启动定时任务系统"，它会：
        - 保存配置信息
        - 创建数据库连接
        - 初始化并启动后台调度器

        工作原理
        --------

        1. **保存配置**：保存传入的配置对象
        2. **创建连接**：创建数据库处理器，建立数据库连接
        3. **启动调度器**：创建并启动后台调度器，准备接收定时任务

        Example:
            >>> scheduler = DataScheduler()
        """
        self.config = config
        
        # 初始化数据库连接，用于数据清洗任务
        self.db_handler = DatabaseHandler(self.config)
        
        # 初始化后台调度器，用于执行定时任务
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()  # 启动调度器

    def _process_data(self):
        """处理数据清洗和入库

        这个函数是定时任务的实际执行函数，它会根据配置的数据源执行相应的数据清洗任务。

        为什么需要这个函数？
        --------------------

        定时任务需要执行具体的清洗逻辑：
        - 清洗股票基本信息（stock 表）
        - 清洗股票市场数据（stock_market 表）
        - 支持多个数据源（RiceQuant、Tushare等）

        这个函数提供了数据清洗的执行逻辑。

        工作原理
        --------

        1. 根据配置的数据源选择相应的清洗器
        2. 清洗股票基本信息（clean_metadata）
        3. 清洗股票市场数据（stock_market_clean_daily）

        Returns:
            None: 函数不返回值，结果通过日志记录

        Note:
            这个函数由调度器自动调用，不需要手动调用
        """
        logger.info(f"Processing data ")
        try:
            data_source = config['DATAHUBSOURCE']
            if data_source == 'ricequant':
                # 清洗stock表当日数据
                stocks_cleaner = RQStockCleaner(self.config)
                stocks_cleaner.clean_metadata()
                # 清洗stock_market表当日数据
                stock_market_cleaner = RQStockMarketCleaner(self.config)
                stock_market_cleaner.stock_market_clean_daily()
            elif data_source == 'tushare':
                # 清洗stock表当日数据
                stocks_cleaner = TSStockCleaner(self.config)
                stocks_cleaner.clean_metadata()
                # 清洗stock_market表当日数据
                stock_market_cleaner = TSStockMarketCleaner(self.config)
                stock_market_cleaner.stock_market_clean_daily()
            # elif data_source == 'xuntou':
            #     # 清洗stock表当日数据
            #     stocks_cleaner = XTStockCleaner(self.config)
            #     stocks_cleaner.clean_metadata()
            #     # 清洗stock_market表当日数据
            #     stock_market_cleaner = XTStockMarketCleaner(self.config)
            #     stock_market_cleaner.stock_market_clean_daily()
        except Exception as e:
            logger.error(f"Error _process_data : {str(e)}")
    
    def schedule_data(self):
        time = self.config["STOCKS_UPDATE_TIME"]
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )
                
        # Add scheduled task
        self.scheduler.add_job(
            self._process_data,
            trigger=trigger,
            id=f"data_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )
        # self._process_data()
        logger.info(f"Scheduled Data")
    
    def stop(self):
        """Stop the scheduler"""
        self.scheduler.shutdown() 

    def reload_schedule(self):
        """重新加载定时任务（用于配置变更后热更新）"""
        self.scheduler.remove_all_jobs()
        self.schedule_data()