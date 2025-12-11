"""
XtQuant 数据下载服务模块

本模块提供了从 XtQuant 数据源下载数据的服务。
它会从 XtQuant 下载股票的价格数据，并存储到本地或数据库。

核心概念
--------

- **数据下载**：从 XtQuant 下载股票价格数据
- **进度回调**：支持进度更新回调，实时显示下载进度
- **单线程下载**：使用单线程顺序下载，避免 API 调用频率限制

为什么需要这个模块？
-------------------

在量化分析中，需要从 XtQuant 下载数据：
- XtQuant 提供了丰富的历史数据
- 需要下载并保存数据供后续使用
- 下载过程可能较长，需要进度反馈

这个模块提供了从 XtQuant 下载数据的能力。

工作原理（简单理解）
------------------

就像下载文件：

1. **连接数据源**：初始化 XtQuant 连接
2. **获取股票列表**：获取要下载的股票列表
3. **逐个下载**：按顺序下载每只股票的数据
4. **保存数据**：将下载的数据保存到指定位置

注意事项
--------

- 使用单线程顺序下载，避免 API 调用频率限制
- 下载过程可能较长，建议使用进度回调显示进度
- 需要 XtQuant 的认证信息（在 config 中配置）
"""

import traceback
from abc import ABC
from xtquant import xtdata
import time
from panda_common.logger_config import logger
from panda_data_hub.utils.xt_utils import XTQuantManager


class XTDownloadService(ABC):
    """XtQuant 数据下载服务

    这个类提供了从 XtQuant 数据源下载数据的功能。
    就像一个"数据下载器"，它会从 XtQuant 下载数据并保存。

    为什么需要这个类？
    -----------------

    在量化分析中，需要从 XtQuant 下载数据：
    - XtQuant 提供了丰富的历史数据
    - 需要下载并保存数据供后续使用
    - 下载过程可能较长，需要进度反馈

    这个类提供了从 XtQuant 下载数据的能力。

    实际使用场景
    -----------

    下载一年的股票价格数据：

    ```python
    service = XTDownloadService(config)
    service.xt_price_data_download("2024-01-01", "2024-12-31")
    ```

    注意事项
    --------

    - 使用单线程顺序下载，避免 API 调用频率限制
    - 下载过程可能较长，建议使用进度回调显示进度
    - 需要 XtQuant 的认证信息（在 config 中配置）
    """
    def __init__(self,config):
        self.config = config
        self.progress_callback = None
        try:
            XTQuantManager.get_instance(config)
            logger.info("XtQuant ready to use")
        except Exception as e:
            error_msg = f"Failed to initialize XtQuant: {str(e)}\nStack trace:\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise

    def set_progress_callback(self, callback):
        """设置进度回调函数"""
        self.progress_callback = callback

    def xt_price_data_download(self, start_date, end_date):
        """单线程顺序下载数据（带进度回调）"""
        try:
            # 获取股票列表
            hs_list = xtdata.get_stock_list_in_sector("沪深A股")
            total = len(hs_list)
            completed = 0
            
            for stock_code in hs_list:
                try:
                    # 下载历史K线
                    xtdata.download_history_data(stock_code, '1d', start_time=start_date, end_time=end_date)
                    # 下载涨跌停价格
                    xtdata.download_history_data(stock_code, 'stoppricedata', start_time=start_date, end_time=end_date)
                    
                    # 更新进度
                    completed += 1
                    progress = int((completed / total) * 100)
                    if self.progress_callback:
                        self.progress_callback(progress)

                    logger.info(f"已下载 {stock_code}，进度: {progress}%")
                    time.sleep(0.1)  # 避免请求过于频繁
                except Exception as e:
                    logger.error(f"下载 {stock_code} 失败: {e}")
                    continue  # 继续下载下一个股票

            logger.info("全部下载完成！")
            if self.progress_callback:
                self.progress_callback(100)  # 确保最终进度为100%

        except Exception as e:
            logger.error(f"下载过程发生错误: {e}")
            if self.progress_callback:
                self.progress_callback(-1)  # 错误信号
            raise  # 重新抛出异常以便上层处理