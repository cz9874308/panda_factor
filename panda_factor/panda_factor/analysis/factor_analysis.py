"""
因子分析模块

本模块提供了因子分析的核心功能，它会对因子进行全面的分析，包括：
- 数据清洗（异常值处理、标准化）
- 因子分组（将股票按因子值分成多组）
- 回测分析（计算各组的收益率、IC、IR等指标）
- 结果保存（将分析结果保存到数据库）

核心概念
--------

- **因子分析**：评估因子的有效性和预测能力
- **因子分组**：将股票按因子值分成多组，分析不同组的收益表现
- **回测**：使用历史数据模拟交易，评估因子策略的表现
- **IC/IR**：信息系数和信息比率，衡量因子的预测能力

为什么需要这个模块？
-------------------

在量化分析中，创建因子后需要评估其有效性：
- 因子是否具有预测能力？
- 因子值高的股票是否收益更高？
- 因子的稳定性如何？

这个模块提供了完整的因子分析流程，帮助用户评估因子的质量。

工作原理（简单理解）
------------------

就像评估一个投资策略：

1. **准备数据**：获取市场数据和因子数据（就像准备历史数据）
2. **清洗数据**：处理异常值和缺失值（就像清理数据）
3. **合并数据**：将市场数据和因子数据合并（就像整合数据）
4. **分组分析**：将股票按因子值分成多组（就像将股票分类）
5. **回测计算**：计算各组的收益率和指标（就像模拟交易）
6. **保存结果**：将分析结果保存到数据库（就像记录结果）

注意事项
--------

- 分析过程可能需要较长时间，特别是数据量大的时候
- 分析结果会保存到数据库，可以通过 task_id 查询
- 分析过程中的状态会实时更新到数据库
"""

import numpy as np
import pandas as pd
import warnings
import logging
import panda_data
from panda_factor.analysis.factor_func import *
from panda_factor.analysis.factor import factor
from tqdm.auto import tqdm  # Import tqdm for progress bars
from typing import Optional, Any
from panda_common.models.factor_analysis_params import Params
from panda_common.handlers.database_handler import DatabaseHandler
from panda_common.config import config
from panda_common.handlers.log_handler import get_factor_logger
import os
from datetime import datetime


def factor_analysis(df_factor: pd.DataFrame, params: Params, factor_id: str = "", task_id: str = "",
                    logger=logging.Logger) -> None:
    """因子分析函数

    这个函数是因子分析的核心，它会执行完整的因子分析流程，包括数据清洗、分组、回测等。
    就像一个"因子评估系统"，它会全面评估因子的有效性和预测能力。

    为什么需要这个函数？
    --------------------

    在量化分析中，创建因子后需要评估其有效性：
    - 因子是否具有预测能力？
    - 因子值高的股票是否收益更高？
    - 因子的稳定性如何？

    这个函数提供了完整的因子分析流程，帮助用户评估因子的质量。

    工作原理（简单理解）
    ------------------

    就像评估一个投资策略：

    1. **准备数据**：获取市场数据和因子数据（就像准备历史数据）
    2. **清洗数据**：处理异常值和缺失值（就像清理数据）
    3. **合并数据**：将市场数据和因子数据合并（就像整合数据）
    4. **分组分析**：将股票按因子值分成多组（就像将股票分类）
    5. **回测计算**：计算各组的收益率和指标（就像模拟交易）
    6. **保存结果**：将分析结果保存到数据库（就像记录结果）

    分析流程
    --------

    1. **初始化**：更新任务状态为"已开始"
    2. **获取K线数据**：从数据库获取市场数据
    3. **清洗因子数据**：处理异常值和标准化
    4. **合并数据**：将市场数据和因子数据合并
    5. **计算滞后收益**：计算未来收益率
    6. **因子分组**：将股票按因子值分成多组
    7. **回测分析**：计算各组的收益率、IC、IR等指标
    8. **保存结果**：将分析结果保存到数据库

    实际使用场景
    -----------

    分析一个动量因子：

    ```python
    from panda_common.models.factor_analysis_params import Params

    params = Params(
        start_date="2024-01-01",
        end_date="2024-12-31",
        stock_pool="000985",
        include_st=True,
        adjustment_cycle=5,
        factor_direction=1,
        group_number=10,
        extreme_value_processing="标准差"
    )

    factor_analysis(
        df_factor=factor_data,
        params=params,
        factor_id="factor_123",
        task_id="task_456",
        logger=logger
    )
    ```

    可能遇到的问题
    ------------

    数据获取失败
    ^^^^^^^^^^^

    如果市场数据获取失败，会抛出异常并更新任务状态为失败。
    检查数据库连接和数据是否存在。

    因子数据异常
    ^^^^^^^^^^^

    如果因子数据有异常（如全为NaN），会在清洗阶段处理。
    检查因子计算是否正确。

    Args:
        df_factor: 因子数据 DataFrame，必须包含 date、symbol 和因子值列
        params: 分析参数对象，包含：
            - start_date: 开始日期
            - end_date: 结束日期
            - stock_pool: 股票池代码
            - include_st: 是否包含ST股票
            - adjustment_cycle: 调仓周期（天数）
            - factor_direction: 因子方向（0表示因子值越小越好，1表示因子值越大越好）
            - group_number: 分组数量（默认10组）
            - extreme_value_processing: 异常值处理方法（"标准差"或"中位数"）
        factor_id: 因子ID，用于关联因子信息（可选）
        task_id: 任务ID，用于跟踪分析进度（必需）
        logger: 日志记录器，用于记录分析过程中的日志

    Returns:
        None: 分析结果会保存到数据库，不直接返回

    Raises:
        Exception: 如果分析过程中出现错误，会更新任务状态为失败并抛出异常

    Example:
        >>> from panda_common.models.factor_analysis_params import Params
        >>> params = Params(
        ...     start_date="2024-01-01",
        ...     end_date="2024-12-31",
        ...     stock_pool="000985",
        ...     include_st=True,
        ...     adjustment_cycle=5,
        ...     factor_direction=1,
        ...     group_number=10
        ... )
        >>> factor_analysis(
        ...     df_factor=factor_data,
        ...     params=params,
        ...     task_id="task_123",
        ...     logger=logger
        ... )
    """
    warnings.filterwarnings("ignore")

    # Get task ID from the task
    _db_handler = DatabaseHandler(config)

    # Update status within the thread
    _db_handler.mongo_update(
        "panda",
        "tasks",
        {"task_id": task_id},
        {
            "process_status": 1,  # Started
            "updated_at": datetime.now().isoformat(),
        }
    )
    # Query factor information
    factor_info = None
    user_id = None
    factor_name = None
    if factor_id:
        factors = _db_handler.mongo_find("panda", "user_factors", {"_id": factor_id})
        if factors and len(factors) > 0:
            factor_info = factors[0]
            user_id = factor_info.get("user_id", "unknown")
            factor_name = factor_info.get("factor_name", "unknown")

    try:
        # Record analysis start
        logger.debug(msg="====== Starting factor analysis ======")

        latest_date = df_factor['date'].max()
        logger.debug(msg=f"Latest date: {latest_date}")

        # Initialize data
        panda_data.init()

        # # Get configuration from parameters
        # # Rebalancing period
        # adjustment_cycle = params.adjustment_cycle if params else 1
        # # Factor direction
        # factor_direction = int(params.factor_direction) if params else 0
        # # Number of groups
        # group_number = params.group_number if params and params.group_number else 10
        # # Extreme value processing method
        # extreme_value_processing = params.extreme_value_processing if params else "Median"

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 2,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        # Get K-line data
        logger.debug(msg="1. Starting to fetch K-line data")
        try:
            df_k_data = panda_data.get_market_data(
                start_date=params.start_date.replace("-", ""),
                end_date=params.end_date.replace("-", ""),
                indicator=params.stock_pool,
                st=params.include_st
            )
            logger.debug(msg=f"k-line data length: {len(df_k_data) if df_k_data is not None else 0}")
            print(df_k_data.tail(5) if df_k_data is not None else "K-line data is None")
            logger.debug(msg="Cleaning K-line data")
            if df_k_data is not None:
                df_k_data_cleaned = clean_k_data(df_k_data)
                logger.debug(msg="Calculating post-adjustment and future returns")
                df_k_data = df_k_data_cleaned.groupby('symbol', group_keys=False).apply(cal_hfq)

        except Exception as e:
            error_msg = f"Failed to fetch K-line data: {str(e)}"
            logger.error(msg=error_msg)
            raise
        logger.debug(
            msg=f"K-line data details - rows: {len(df_k_data) if df_k_data is not None else 0}, symbols: {len(df_k_data['symbol'].unique()) if df_k_data is not None else 0}")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 3,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )
        # Cleaning factor data
        logger.debug(msg="2. Starting to clean factor data")
        try:
            factor_list = [df_factor.columns[2]]  # Get the name of the third column and convert to list
            logger.info(msg=f"Factor list: {factor_list}")

            # Choose extreme value processing method based on parameters
            if params.extreme_value_processing == "标准差" or params.extreme_value_processing == "std":
                logger.info(msg="Using ext_out_3std method for extreme value processing")
                df_factor = df_factor.groupby('date', group_keys=False).apply(
                    lambda x: ext_out_3std_list(x, factor_list))  # 3-sigma extreme value processing
            else:  # Default to median method
                logger.info(msg="Using ext_out_mad method for extreme value processing")
                df_factor = df_factor.groupby('date', group_keys=False).apply(
                    lambda x: ext_out_3std_list(x, factor_list))  # Median extreme value processing

            logger.info(msg="Starting z_score processing")
            df_factor = df_factor.groupby('date', group_keys=False).apply(
                lambda x: z_score(x, factor_list))  # z-score standardization
        except Exception as e:
            error_msg = f"Failed to clean factor data: {str(e)}"
            logger.error(msg=error_msg, extra={"stage": "data_cleaning"})
            raise
        logger.debug(
            msg=f"Factor data cleaning details stage: data_cleaning, rows: {len(df_factor) if df_factor is not None else 0}")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 4,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        # Merge data
        logger.debug(msg="3. Starting to merge data")
        try:
            df = pd.merge(df_k_data, df_factor, on=['date', 'symbol'], how='left')
            print(len(df))
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
            df = df[df[factor_list].notna().all(axis=1)]
            df = df[df[f'{params.adjustment_cycle}day_return'].notna()]
        except Exception as e:
            error_msg = f"merge data failed: {str(e)}"
            logger.error(msg=error_msg)
            raise
        print(df.tail(5))
        logger.debug(msg=f"Data merge details, rows: {len(df) if df is not None else 0}")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 5,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        # Calculate lagged returns
        logger.debug(msg="4. Starting to calculate lagged returns")
        try:
            df = cal_pct_lag(df)
        except Exception as e:
            error_msg = f"Failed to calculate lagged returns: {str(e)}"
            logger.error(msg=error_msg, extra={"stage": "return_calculation"})
            raise

        logger.debug(msg="Lagged returns calculation completed")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 6,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        # Factor data grouping
        logger.info(msg=f"5. Starting factor data grouping, group number: {params.group_number}")
        try:
            # Use group number from parameters
            df_cuted, df_benchmark = grouping_factor(df, factor_list[0], params.group_number, logger)
        except Exception as e:
            error_msg = f"Factor data grouping failed: {str(e)}"
            logger.error(msg=error_msg, extra={"stage": "grouping"})
            raise

        logger.debug(
            msg=f"Factor grouping details, group number: {params.group_number}, benchmark date count: {len(df_benchmark) if df_benchmark is not None else 0}")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 7,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

        def enrich_stock_data(df):
            # Get all unique stock codes
            symbols = df["symbol"].unique().tolist()

            # Query stock names from MongoDB
            query = {'symbol': {'$in': symbols}, 'expired': False}
            cursor = _db_handler.mongo_find(
                "panda",
                "stocks",  # Modify to the correct collection name
                query
            )

            # Create symbol to name mapping dictionary
            symbol_to_name = {item['symbol']: item['name'] for item in cursor}

            # Copy original DataFrame
            result_df = df.copy()

            # Add name column
            result_df['name'] = result_df["symbol"].map(symbol_to_name)

            return result_df

        last_date_top_factor_tmp = df_factor[df_factor['date'] == latest_date].sort_values(by=factor_list[0],
                                                                                           ascending=False).head(20)
        last_date_top_factor_tmp = enrich_stock_data(last_date_top_factor_tmp)
        # Progress bar: In-depth factor analysis
        logger.debug(msg="6. Starting in-depth factor analysis")
        factor_obj_list = []
        for f in factor_list:
            try:
                logger.debug(msg=f"Analyzing factor {f}")
                # Create factor object with group number
                factor_obj = factor(f, group_number=params.group_number, factor_id=factor_id)
                # Top 20 factor values for the latest date

                factor_obj.last_date_top_factor = last_date_top_factor_tmp
                factor_obj.logger = logger
                logger.debug(msg=f"Retrieved Top20 factor values for latest date {latest_date}")

                factor_obj_list.append(
                    factor_obj)  # Create factor class object to store various backtest parameters and results
                # :param predict_direction: Prediction direction (0 for smaller factor value is better, IC is negative/1 for larger factor value is better, IC is positive)
                factor_obj.set_backtest_parameters(period=params.adjustment_cycle,
                                                   predict_direction=params.factor_direction, commission=0)

                logger.debug(
                    msg=f"Set backtest parameters: period={params.adjustment_cycle}, predict_direction={params.factor_direction}, commission=0")
                logger.debug(msg=f"Starting backtest for factor {f}")
                factor_obj.start_backtest(df_cuted, df_benchmark)
                logger.debug(msg=f"Completed backtest for factor {f}")
                logger.debug(msg=f"7. Saving analysis results for factor {f} to database...")
                # Update status within the thread
                _db_handler.mongo_update(
                    "panda",
                    "tasks",
                    {"task_id": task_id},
                    {
                        "process_status": 8,  # Started
                        "updated_at": datetime.now().isoformat(),
                    }
                )
                factor_obj.inset_to_database(factor_id, task_id)
                logger.debug(msg=f"Analysis results for factor {f} saved")
            except Exception as e:
                error_msg = f"Factor {f} analysis failed: {str(e)}"
                logger.error(msg=error_msg, extra={"stage": "factor_analysis"})
                raise

        logger.debug(msg="In-depth factor analysis completed", extra={"stage": "factor_analysis"})

        logger.debug(msg="======= Factor analysis completed =======")

        # Update status within the thread
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": 9,  # Started
                "updated_at": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        # Record overall error
        error_msg = f"factor analysis failed: {str(e)}"
        logger.error(msg=error_msg, extra={"stage": "error"})
        # Update task status to failed
        _db_handler.mongo_update(
            "panda",
            "tasks",
            {"task_id": task_id},
            {
                "process_status": -1,  # Failed
                "error_message": error_msg,
                "updated_at": datetime.now().isoformat(),
            }
        )
        raise  # Re-raise exception
