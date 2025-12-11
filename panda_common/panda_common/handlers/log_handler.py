"""
日志处理模块

本模块提供了因子分析专用的日志处理功能，就像一个"日志管家"，
它会将因子分析过程中的日志信息保存到 MongoDB 数据库中，并支持批量写入，
让你可以方便地追踪和分析因子计算的整个过程。

核心概念
--------

- **LogBatchManager**：日志批量管理器，使用单例模式，负责批量缓存和写入日志
- **FactorAnalysisLogHandler**：因子分析日志处理器，继承自 logging.Handler
- **批量写入**：将日志先缓存起来，然后批量写入数据库，提高性能
- **实时写入**：对于重要的日志（ERROR、CRITICAL、WARNING），立即写入数据库

使用方式
--------

1. 使用 `get_factor_logger()` 获取专用的日志记录器
2. 使用标准的 logging API 记录日志
3. 日志会自动保存到 MongoDB 数据库

工作原理
--------

就像邮局的邮件处理系统：

1. **收集邮件**：日志处理器收集所有日志信息（就像邮局收集邮件）
2. **分类缓存**：按任务 ID 分类缓存日志（就像按地区分类邮件）
3. **批量发送**：定期批量写入数据库（就像定期批量发送邮件）
4. **紧急处理**：重要日志立即写入（就像加急邮件立即发送）

注意事项
--------

- 使用单例模式确保整个应用只有一个日志批量管理器
- 使用线程锁保证多线程环境下的数据安全
- 后台线程定期刷新日志，确保日志不会丢失
- 应用关闭时会自动保存所有缓存的日志
"""

import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.config import config
import threading
import time
import os


class LogBatchManager:
    """日志批量管理器：缓存日志并定期批量写入数据库

    这个类就像一个"日志缓存中心"，它会将日志先缓存起来，然后定期批量写入数据库，
    这样可以大大提高日志写入的性能，避免频繁的数据库操作。

    为什么需要这个类？
    --------------------

    在因子分析过程中，会产生大量的日志信息：
    - 每个步骤的进度信息
    - 数据处理的详细信息
    - 错误和警告信息

    如果每条日志都立即写入数据库，会导致：
    - 数据库操作过于频繁，性能下降
    - 数据库连接数过多，资源浪费
    - 系统响应变慢，影响用户体验

    这个类通过批量写入解决了这些问题。

    工作原理（简单理解）
    ------------------

    就像邮局的邮件处理系统：

    1. **收集邮件**：接收所有日志信息（就像邮局收集邮件）
    2. **分类缓存**：按任务 ID 分类缓存日志（就像按地区分类邮件）
    3. **定期发送**：后台线程每 5 秒批量写入一次（就像定期批量发送邮件）
    4. **紧急处理**：重要日志立即写入（就像加急邮件立即发送）
    5. **容量控制**：每个任务的缓存达到 50 条时立即写入（就像邮件箱满了就发送）

    实际使用场景
    -----------

    在因子分析过程中自动使用：

    ```python
    # 获取日志记录器
    logger = get_factor_logger(task_id, factor_id)

    # 记录日志（会自动批量写入数据库）
    logger.info("开始因子分析")
    logger.debug("数据加载完成", extra={"stage": "data_loading"})
    logger.error("计算失败", extra={"stage": "calculation"})
    ```

    可能遇到的问题
    ------------

    日志丢失
    ^^^^^^^

    如果应用异常退出，缓存中的日志可能会丢失。
    应用正常关闭时会自动调用 `shutdown()` 方法保存所有日志。

    内存占用
    ^^^^^^^

    如果日志产生速度过快，缓存可能会占用较多内存。
    系统会自动在缓存达到阈值时立即写入，控制内存占用。

    Attributes:
        log_buffer: 日志缓存字典，按任务 ID 分组存储日志
        buffer_lock: 线程锁，保证多线程环境下的数据安全
        db_handler: 数据库处理器实例
        flush_interval: 刷新间隔（秒），默认 5 秒
        max_buffer_size: 每个任务的最大缓存数量，默认 50 条
        stop_flag: 停止标志，用于控制后台线程
        flush_thread: 后台刷新线程
    """
    
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls):
        """获取单例实例

        这个方法实现了单例模式，确保整个应用只有一个日志批量管理器实例。

        为什么需要单例模式？
        --------------------

        日志批量管理器需要统一管理所有日志的缓存和写入，如果创建多个实例，
        会导致日志分散，无法统一管理，也无法有效利用批量写入的优势。

        工作原理
        --------

        使用双重检查锁定模式（Double-Checked Locking）：
        1. 第一次检查：快速检查实例是否已存在（不加锁）
        2. 加锁：如果不存在，获取锁
        3. 第二次检查：再次检查实例是否已存在（加锁后）
        4. 创建实例：如果确实不存在，创建新实例

        Returns:
            LogBatchManager: 日志批量管理器实例（单例）
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = LogBatchManager()
        return cls._instance
    
    def __init__(self):
        """初始化批量管理器

        这个方法负责初始化日志缓存和启动后台刷新线程。

        工作原理
        --------

        1. 初始化日志缓存字典（按任务 ID 分组）
        2. 创建线程锁（保证多线程安全）
        3. 创建数据库处理器实例
        4. 设置刷新间隔和缓存大小限制
        5. 启动后台线程定期刷新日志

        Note:
            - 使用守护线程，应用退出时自动结束
            - 默认每 5 秒刷新一次日志
            - 每个任务最多缓存 50 条日志
        """
        self.log_buffer = {}  # 日志缓存，按任务 ID 分组存储
        self.buffer_lock = threading.Lock()  # 线程锁，保证多线程环境下的数据安全
        self.db_handler = DatabaseHandler(config)  # 数据库处理器实例
        self.flush_interval = 5  # 刷新间隔（秒），每 5 秒自动刷新一次
        self.max_buffer_size = 50  # 每个任务的最大缓存数量，达到此数量时立即刷新
        
        # 启动后台线程定期刷新日志
        # 使用守护线程，应用退出时自动结束
        self.stop_flag = False
        self.flush_thread = threading.Thread(target=self._flush_loop)
        self.flush_thread.daemon = True
        self.flush_thread.start()
    
    def add_log(self, task_id: str, log_entry: Dict[str, Any]):
        """添加日志到缓存

        这个函数就像一个"邮件收集箱"，它会将日志添加到缓存中，
        如果缓存达到阈值，会立即刷新到数据库。

        为什么需要这个函数？
        --------------------

        在因子分析过程中，会产生大量的日志信息。如果每条日志都立即写入数据库，
        会导致数据库操作过于频繁，影响性能。这个函数先将日志缓存起来，
        然后批量写入，大大提高性能。

        工作原理
        --------

        1. 获取线程锁（保证多线程安全）
        2. 检查任务 ID 是否已存在于缓存中，如果不存在则创建新的列表
        3. 将日志添加到对应任务的缓存列表中
        4. 如果缓存达到阈值（50 条），立即刷新到数据库

        Args:
            task_id: 任务 ID，用于区分不同任务的日志
            log_entry: 日志条目字典，包含日志的所有信息

        Example:
            >>> manager = LogBatchManager.get_instance()
            >>> log_entry = {
            ...     "task_id": "123",
            ...     "message": "开始分析",
            ...     "level": "INFO"
            ... }
            >>> manager.add_log("123", log_entry)
        """
        with self.buffer_lock:
            # 如果任务 ID 不存在，创建新的缓存列表
            if task_id not in self.log_buffer:
                self.log_buffer[task_id] = []
            
            # 将日志添加到缓存
            self.log_buffer[task_id].append(log_entry)
            
            # 如果缓存达到阈值，立即刷新到数据库
            # 这样可以避免缓存过大占用过多内存
            if len(self.log_buffer[task_id]) >= self.max_buffer_size:
                self._flush_task_logs(task_id)
    
    def _flush_loop(self):
        """后台线程循环：定期刷新所有日志

        这个函数在后台线程中运行，定期刷新所有任务的日志到数据库。

        为什么需要这个函数？
        --------------------

        即使缓存没有达到阈值，我们也需要定期刷新日志，确保日志不会长时间停留在缓存中。
        这样可以：
        - 及时保存日志，避免丢失
        - 控制缓存大小，避免内存占用过大
        - 提供实时的日志查看能力

        工作原理
        --------

        1. 循环检查停止标志
        2. 如果未停止，等待指定的刷新间隔（默认 5 秒）
        3. 刷新所有任务的日志到数据库
        4. 重复步骤 1-3

        Note:
            - 这是一个后台线程函数，由 `__init__` 方法启动
            - 使用守护线程，应用退出时自动结束
        """
        while not self.stop_flag:
            # 等待指定的刷新间隔
            time.sleep(self.flush_interval)
            # 刷新所有任务的日志
            self.flush_all()
    
    def _flush_task_logs(self, task_id: str):
        """刷新指定任务的日志到数据库

        这个函数就像一个"邮件发送员"，它会将指定任务的所有缓存日志写入数据库，
        并更新任务的状态信息。

        为什么需要这个函数？
        --------------------

        当缓存达到阈值或定期刷新时，需要将日志写入数据库。
        这个函数负责：
        - 将缓存中的日志写入数据库
        - 更新任务的最新状态信息
        - 清空缓存，释放内存

        工作原理
        --------

        1. 获取线程锁，从缓存中取出日志（保证线程安全）
        2. 清空缓存，释放内存
        3. 遍历日志列表，为每条日志创建完整的记录
        4. 将日志记录插入数据库
        5. 使用最新日志更新任务状态

        Args:
            task_id: 任务 ID，指定要刷新哪个任务的日志

        Note:
            - 如果写入失败，会打印错误信息，但不会抛出异常
            - 这样可以避免因为日志写入失败导致整个应用崩溃
        """
        logs_to_save = []
        # 获取线程锁，从缓存中取出日志
        with self.buffer_lock:
            if task_id in self.log_buffer and self.log_buffer[task_id]:
                logs_to_save = self.log_buffer[task_id]
                # 清空缓存，释放内存
                self.log_buffer[task_id] = []
        
        if logs_to_save:
            try:
                # 遍历所有日志，写入数据库
                for log in logs_to_save:
                    # 构建完整的日志记录
                    log_record = {
                        "log_id": str(uuid.uuid4()),  # 生成唯一的日志 ID
                        "task_id": log["task_id"],
                        "factor_id": log["factor_id"],
                        "message": log["message"],
                        "level": log["level"],
                        "timestamp": log["timestamp"],
                        "stage": log.get("stage", "default"),  # 分析阶段，默认为 "default"
                        "details": log.get("details"),  # 详细信息（可选）
                        "created_at": datetime.now().isoformat(),
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    # 直接插入数据库
                    self.db_handler.mongo_insert("panda", "factor_analysis_stage_logs", log_record)
                
                # 更新任务状态
                # 使用最新日志的信息更新任务状态，方便实时查看任务进度
                if len(logs_to_save) > 0:
                    latest_log = logs_to_save[-1]
                    self.db_handler.mongo_update(
                        "panda",
                        "tasks",
                        {"task_id": latest_log["task_id"]},
                        {
                            "current_stage": latest_log.get("stage", "unknown"),  # 当前分析阶段
                            "last_log_message": latest_log["message"],  # 最新日志消息
                            "last_log_time": latest_log["timestamp"],  # 最新日志时间
                            "last_log_level": latest_log["level"],  # 最新日志级别
                            "updated_at": datetime.now().isoformat()
                        }
                    )
            except Exception as e:
                # 写入失败时打印错误信息，但不抛出异常
                # 这样可以避免因为日志写入失败导致整个应用崩溃
                print(f"Error saving batch logs: {e}")
    
    def flush_all(self):
        """刷新所有任务的日志到数据库

        这个函数会刷新所有任务的缓存日志到数据库，通常在定期刷新或应用关闭时调用。

        为什么需要这个函数？
        --------------------

        在以下情况下需要刷新所有日志：
        - 定期刷新（每 5 秒）
        - 应用关闭时
        - 重要日志需要立即写入时

        工作原理
        --------

        1. 获取所有任务 ID 的列表（加锁保证线程安全）
        2. 遍历所有任务，逐个刷新日志

        Example:
            >>> manager = LogBatchManager.get_instance()
            >>> manager.flush_all()  # 刷新所有任务的日志
        """
        # 获取所有任务 ID 的列表（加锁保证线程安全）
        with self.buffer_lock:
            task_ids = list(self.log_buffer.keys())
        
        # 遍历所有任务，逐个刷新日志
        for task_id in task_ids:
            self._flush_task_logs(task_id)
    
    def shutdown(self):
        """关闭批量管理器

        这个函数用于优雅地关闭批量管理器，确保所有日志都被保存。

        为什么需要这个函数？
        --------------------

        当应用关闭时，需要确保所有缓存的日志都被保存到数据库，避免日志丢失。

        工作原理
        --------

        1. 设置停止标志，通知后台线程停止
        2. 等待后台线程结束（最多等待 10 秒）
        3. 刷新所有日志，确保所有日志都被保存

        Note:
            - 应用关闭时应该调用此方法
            - 确保所有日志都被保存后才退出
        """
        # 设置停止标志，通知后台线程停止
        self.stop_flag = True
        # 等待后台线程结束（最多等待 10 秒）
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=10)
        # 刷新所有日志，确保所有日志都被保存
        self.flush_all()

class FactorAnalysisLogHandler(logging.Handler):
    """因子分析日志处理器：将日志保存到 MongoDB

    这个类继承自 `logging.Handler`，是一个自定义的日志处理器，
    它会将因子分析过程中的所有日志信息保存到 MongoDB 数据库中。

    为什么需要这个类？
    --------------------

    在因子分析过程中，需要记录大量的日志信息：
    - 分析进度
    - 数据处理状态
    - 错误和警告

    标准的日志处理器只能输出到控制台或文件，无法保存到数据库。
    这个类提供了将日志保存到数据库的能力，方便后续查询和分析。

    工作原理（简单理解）
    ------------------

    就像邮局的邮件处理系统：

    1. **接收邮件**：接收所有日志记录（就像邮局接收邮件）
    2. **分类处理**：根据日志级别和内容分类处理（就像按邮件类型分类）
    3. **批量发送**：将日志添加到批量管理器（就像将邮件放入发送队列）
    4. **紧急处理**：重要日志立即发送（就像加急邮件立即发送）

    实际使用场景
    -----------

    在因子分析过程中自动使用：

    ```python
    # 获取日志记录器（会自动使用此处理器）
    logger = get_factor_logger(task_id, factor_id)

    # 记录日志（会自动保存到数据库）
    logger.info("开始因子分析")
    logger.debug("数据加载完成", extra={"stage": "data_loading"})
    logger.error("计算失败", extra={"stage": "calculation"})
    ```

    Attributes:
        task_id: 任务 ID，用于标识日志属于哪个任务
        factor_id: 因子 ID，用于标识日志属于哪个因子
        batch_manager: 日志批量管理器实例
        formatter: 日志格式化器
    """

    def __init__(self, task_id: str, factor_id: str, level=logging.INFO):
        """初始化日志处理器

        这个方法负责初始化日志处理器的各个属性。

        Args:
            task_id: 任务 ID，用于标识日志属于哪个任务
            factor_id: 因子 ID，用于标识日志属于哪个因子
            level: 日志级别，默认 INFO

        Note:
            - 使用日志批量管理器来批量写入日志
            - 创建日志格式化器用于格式化日志消息
        """
        super().__init__(level)
        self.task_id = task_id
        self.factor_id = factor_id
        # 获取日志批量管理器实例（单例）
        self.batch_manager = LogBatchManager.get_instance()
        
        # 创建日志格式化器
        # 格式：时间 - 级别 - 消息
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
    def emit(self, record):
        """处理日志记录并保存到 MongoDB

        这个函数是日志处理器的核心方法，当有日志记录时会被自动调用。

        为什么需要这个函数？
        --------------------

        这是 `logging.Handler` 的标准接口，当日志记录器记录日志时，
        会自动调用此方法处理日志。我们需要在这里将日志保存到数据库。

        工作原理
        --------

        1. 从日志记录中提取信息（级别、消息、阶段、详细信息等）
        2. 构建日志条目，添加到批量管理器
        3. 如果有详细信息，为每个字段创建单独的 DEBUG 日志
        4. 如果是重要日志（ERROR、CRITICAL、WARNING），立即刷新到数据库

        Args:
            record: 日志记录对象，包含日志的所有信息

        Note:
            - 如果处理失败，会打印错误信息，但不会抛出异常
            - 这样可以避免因为日志处理失败导致整个应用崩溃
        """
        try:
            # 从日志记录的 extra 字段中获取额外信息
            # stage: 分析阶段，如 "data_loading", "calculation" 等
            stage = getattr(record, 'stage', 'default')
            # details: 详细信息字典，包含额外的调试信息
            details = getattr(record, 'details', {})
            
            # 构建主日志条目
            log_entry = {
                "log_id": str(uuid.uuid4()),  # 生成唯一的日志 ID
                "task_id": self.task_id,
                "factor_id": self.factor_id,
                "level": record.levelname,  # 日志级别：DEBUG、INFO、WARNING、ERROR、CRITICAL
                "message": self.format(record),  # 格式化后的日志消息
                "timestamp": datetime.now().isoformat(),  # 时间戳
                "stage": stage  # 分析阶段
            }
            
            # 添加到批量管理器（会缓存起来，定期批量写入）
            self.batch_manager.add_log(self.task_id, log_entry)
            
            # 如果有详细信息，为每个字段创建单独的 DEBUG 日志
            # 这样可以方便后续查询和分析详细信息
            if details:
                for key, value in details.items():
                    debug_entry = {
                        "log_id": str(uuid.uuid4()),
                        "task_id": self.task_id,
                        "factor_id": self.factor_id,
                        "level": "DEBUG",
                        "message": f"{key}: {value}",
                        "timestamp": datetime.now().isoformat(),
                        "stage": stage
                    }
                    self.batch_manager.add_log(self.task_id, debug_entry)
            
            # 对于重要日志（ERROR、CRITICAL、WARNING），立即刷新到数据库
            # 这样可以确保重要信息不会被延迟，方便实时查看
            if record.levelname in ("ERROR", "CRITICAL", "WARNING"):
                self.batch_manager.flush_all()
                
        except Exception as e:
            # 避免因为日志处理异常导致应用崩溃
            # 如果日志处理失败，打印错误信息，但继续执行
            print(f"Error saving log to MongoDB: {e}")
            
def get_factor_logger(task_id: str, factor_id: str) -> logging.Logger:
    """获取因子分析专用的日志记录器

    这个函数就像一个"日志记录器工厂"，它会创建一个专门用于因子分析的日志记录器，
    配置了控制台输出和 MongoDB 存储两种处理器。

    为什么需要这个函数？
    --------------------

    在因子分析过程中，需要记录大量的日志信息，并且需要：
    - 在控制台实时查看日志（方便调试）
    - 将日志保存到数据库（方便后续查询和分析）

    这个函数提供了统一的日志记录器创建方式，确保所有因子分析都使用相同的日志配置。

    工作原理（简单理解）
    ------------------

    就像配置一个"双通道录音设备"：

    1. **创建录音设备**：创建日志记录器（就像创建录音设备）
    2. **配置控制台通道**：添加控制台处理器（就像配置实时监听通道）
    3. **配置数据库通道**：添加 MongoDB 处理器（就像配置存储通道）
    4. **返回设备**：返回配置好的日志记录器（就像返回录音设备）

    实际使用场景
    -----------

    在因子分析开始时调用：

    ```python
    # 获取日志记录器
    logger = get_factor_logger(task_id, factor_id)

    # 记录日志（会自动输出到控制台和保存到数据库）
    logger.info("开始因子分析")
    logger.debug("数据加载完成", extra={"stage": "data_loading"})
    logger.error("计算失败", extra={"stage": "calculation", "details": {"error": "..."}})
    ```

    日志级别说明
    -----------

    - **DEBUG**：详细的调试信息，包括数据处理的详细信息
    - **INFO**：一般信息，如分析进度、步骤完成等
    - **WARNING**：警告信息，如数据异常但可以继续处理
    - **ERROR**：错误信息，如计算失败、数据缺失等
    - **CRITICAL**：严重错误，如系统崩溃、数据损坏等

    Args:
        task_id: 任务 ID，用于标识日志属于哪个任务
        factor_id: 因子 ID，用于标识日志属于哪个因子

    Returns:
        logging.Logger: 配置好的日志记录器，包含控制台和 MongoDB 两种处理器

    Example:
        >>> logger = get_factor_logger("task_123", "factor_456")
        >>> logger.info("开始分析")
        >>> logger.debug("数据加载完成", extra={"stage": "data_loading"})
    """
    # 创建日志记录器
    # 使用任务 ID 作为日志记录器的名称，确保每个任务有独立的日志记录器
    logger = logging.getLogger(f"factor_analysis_{task_id}")
    logger.setLevel(logging.DEBUG)  # 设置日志级别为 DEBUG，记录所有级别的日志
    
    # 如果处理器已存在，直接返回（避免重复添加处理器）
    # 这通常发生在多次调用此函数时
    if logger.handlers:
        return logger
        
    # 创建控制台处理器
    # 用于在控制台实时查看日志，方便调试
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  # 控制台显示所有级别的日志
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 创建 MongoDB 处理器
    # 用于将日志保存到数据库，方便后续查询和分析
    mongo_handler = FactorAnalysisLogHandler(
        task_id=task_id,
        factor_id=factor_id
    )
    mongo_handler.setLevel(logging.DEBUG)  # MongoDB 记录所有级别的日志
    # MongoDB 处理器使用简化的格式化器，只保存消息内容
    # 其他信息（如级别、时间戳等）会在保存时单独添加
    mongo_formatter = logging.Formatter('%(message)s')
    mongo_handler.setFormatter(mongo_formatter)
    logger.addHandler(mongo_handler)
    
    return logger