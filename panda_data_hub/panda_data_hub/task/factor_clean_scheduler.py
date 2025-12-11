"""
因子数据清洗任务调度模块

本模块提供了因子数据清洗任务的定时调度功能，使用 APScheduler 实现定时任务。
它会按照配置的时间自动执行因子数据清洗任务，保持因子数据的及时更新。

核心概念
--------

- **任务调度**：使用 APScheduler 实现定时任务调度
- **定时清洗**：按照配置的时间自动执行因子数据清洗
- **后台运行**：调度器在后台运行，不阻塞主程序

为什么需要这个模块？
-------------------

在量化分析中，需要定期更新因子数据：
- 每天需要清洗最新的因子数据
- 需要自动执行，无需人工干预
- 需要支持多个数据源

这个模块提供了定时任务调度的能力。

工作原理（简单理解）
------------------

就像定时闹钟：

1. **设置闹钟**：配置任务执行时间（就像设置闹钟时间）
2. **等待触发**：调度器等待到指定时间（就像等待闹钟响起）
3. **执行任务**：自动执行因子数据清洗任务（就像执行闹钟提醒的事情）
4. **循环执行**：每天重复执行（就像每天都会响的闹钟）

注意事项
--------

- 调度器在后台运行，不会阻塞主程序
- 任务执行时间由 config 中的 FACTOR_UPDATE_TIME 配置
- 支持多个数据源，根据 DATAHUBSOURCE 配置选择
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import datetime
from panda_common.config import config, logger
from panda_common.handlers.database_handler import DatabaseHandler
from panda_data_hub.factor.rq_factor_clean_pro import RQFactorCleaner
from panda_data_hub.factor.ts_factor_clean_pro import TSFactorCleaner
# from panda_data_hub.factor.xt_factor_clean_pro import XTFactorCleaner


class FactorCleanerScheduler():
    """因子数据清洗任务调度器

    这个类就像一个"定时任务管理器"，它会按照配置的时间自动执行因子数据清洗任务。
    使用 APScheduler 实现定时调度，支持 Cron 表达式。

    为什么需要这个类？
    -----------------

    在量化分析中，需要定期更新因子数据：
    - 每天需要清洗最新的因子数据
    - 需要自动执行，无需人工干预
    - 需要支持多个数据源

    这个类提供了定时任务调度的能力。

    工作原理（简单理解）
    ------------------

    就像定时闹钟：

    1. **设置闹钟**：配置任务执行时间（就像设置闹钟时间）
    2. **等待触发**：调度器等待到指定时间（就像等待闹钟响起）
    3. **执行任务**：自动执行因子数据清洗任务（就像执行闹钟提醒的事情）
    4. **循环执行**：每天重复执行（就像每天都会响的闹钟）

    实际使用场景
    -----------

    每天下午4点自动清洗因子数据：

    ```python
    scheduler = FactorCleanerScheduler()
    scheduler.schedule_data()  # 配置并启动定时任务
    ```

    注意事项
    --------

    - 调度器在后台运行，不会阻塞主程序
    - 任务执行时间由 config 中的 FACTOR_UPDATE_TIME 配置
    - 支持多个数据源，根据 DATAHUBSOURCE 配置选择
    """

    def __init__(self):
        """初始化因子数据调度器

        这个函数就像"启动定时任务系统"，它会：
        - 保存配置信息
        - 创建数据库连接
        - 初始化并启动后台调度器

        Example:
            >>> scheduler = FactorCleanerScheduler()
        """
        self.config = config

        # 初始化数据库连接，用于因子数据清洗任务
        self.db_handler = DatabaseHandler(self.config)
        # 初始化后台调度器，用于执行定时任务
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()  # 启动调度器

    def _process_factor(self):
        """处理因子数据清洗

        这个函数是定时任务的实际执行函数，它会根据配置的数据源执行相应的因子数据清洗任务。

        为什么需要这个函数？
        --------------------

        定时任务需要执行具体的清洗逻辑：
        - 清洗因子数据
        - 支持多个数据源（RiceQuant、Tushare等）

        这个函数提供了因子数据清洗的执行逻辑。

        工作原理
        --------

        1. 根据配置的数据源选择相应的因子清洗器
        2. 执行每日因子数据清洗（clean_daily_factor）

        Returns:
            None: 函数不返回值，结果通过日志记录

        Note:
            这个函数由调度器自动调用，不需要手动调用
        """
        logger.info(f"Processing data ")
        try:
            data_source = config['DATAHUBSOURCE']
            if data_source == 'ricequant':
                # 清洗因子数据
                factor_cleaner = RQFactorCleaner(self.config)
                factor_cleaner.clean_daily_factor()
            elif data_source == 'tushare':
                # 清洗因子数据
                factor_cleaner = TSFactorCleaner(self.config)
                factor_cleaner.clean_daily_factor()
            # if data_source == 'xuntou':
            #     factor_cleaner = XTFactorCleaner(self.config)
            #     factor_cleaner.clean_daily_factor()

        except Exception as e:
            logger.error(f"Error _process_data : {str(e)}")

    def schedule_data(self):
        time = self.config["FACTOR_UPDATE_TIME"]
        hour, minute = time.split(":")
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day='*',
            month='*',
            day_of_week='*'
        )

        # 添加定时任务
        self.scheduler.add_job(
            self._process_factor,
            trigger=trigger,
            id=f"data_{datetime.datetime.now().strftime('%Y%m%d')}",
            replace_existing=True
        )
        # self._process_factor()
        logger.info(f"Scheduled Data")

    def stop(self):
        """停止调度器"""
        self.scheduler.shutdown()


















