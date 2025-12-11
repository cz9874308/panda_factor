"""
配置管理模块

本模块负责加载和管理整个项目的配置信息，就像一个"配置管家"，
它会从配置文件中读取基础配置，然后允许通过环境变量进行覆盖，
让你可以在不同环境中灵活调整配置，而不用修改代码。

核心概念
--------

- **配置文件**：`config.yaml` 文件，存储项目的默认配置
- **环境变量**：系统环境变量，可以覆盖配置文件中的值
- **配置优先级**：环境变量 > 配置文件，环境变量的优先级更高

使用方式
--------

1. 配置文件会自动从 `config.yaml` 加载
2. 可以通过设置环境变量来覆盖配置值
3. 使用 `get_config()` 函数获取配置对象

工作原理
--------

就像整理房间的规则：
1. **先铺基础**：从 `config.yaml` 文件加载所有默认配置（就像先把房间的基础布置好）
2. **再个性化**：检查环境变量，如果有就覆盖配置文件的值（就像根据个人喜好调整细节）
3. **统一管理**：所有配置都存储在全局 `config` 变量中，方便统一访问

注意事项
--------

- 配置文件必须使用 UTF-8 编码
- 环境变量的值会自动转换为与配置文件相同的类型
- 如果配置文件加载失败，会抛出 `FileNotFoundError` 异常
- 如果环境变量类型转换失败，会保持配置文件中的原值
"""
import os
import yaml
import logging
from pathlib import Path

# 获取logger
# 优先使用项目统一的日志配置，如果无法导入则使用标准库的logging
try:
    from panda_common.logger_config import logger
except ImportError:
    # 如果无法导入logger，创建一个基本的logger
    # 这通常发生在模块被单独使用时
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("config")

# 初始化配置变量
# 使用全局变量存储配置，实现单例模式
config = None

def load_config():
    """加载配置文件，并从环境变量更新配置

    这个函数就像一个"配置加载器"，它会先读取配置文件中的默认设置，
    然后检查环境变量，如果环境变量中有对应的值，就用环境变量的值覆盖配置文件的值。

    为什么需要这个函数？
    --------------------

    在实际开发中，我们经常需要在不同环境中使用不同的配置：
    - 开发环境：使用本地数据库
    - 测试环境：使用测试数据库
    - 生产环境：使用生产数据库

    如果每次都修改配置文件会很麻烦，而且容易出错。
    这个函数让你可以通过设置环境变量来灵活调整配置，而不需要修改代码或配置文件。

    工作原理（简单理解）
    ------------------

    就像点餐时的个性化定制：

    1. **读取默认菜单**：从 `config.yaml` 文件加载所有默认配置
       （就像餐厅的默认菜单，有标准配置）

    2. **检查个性化需求**：遍历配置文件中的所有键，检查环境变量中是否有对应的值
       （就像询问客人是否有特殊要求）

    3. **应用个性化设置**：如果环境变量中有值，就覆盖配置文件中的值
       （就像根据客人的要求调整菜品）

    4. **类型转换**：自动将环境变量的字符串值转换为配置文件中的原始类型
       （就像把"不要辣"转换成系统能理解的格式）

    实际使用场景
    -----------

    通常在应用启动时自动调用，也可以在需要时手动调用：

    ```python
    # 自动调用（模块导入时）
    from panda_common.config import get_config
    config = get_config()

    # 手动调用
    from panda_common.config import load_config
    config = load_config()
    ```

    可能遇到的问题
    ------------

    配置文件不存在
    ^^^^^^^^^^^^^

    如果 `config.yaml` 文件不存在或无法读取，函数会抛出 `FileNotFoundError` 异常。
    确保配置文件存在于 `panda_common/panda_common/` 目录下。

    环境变量类型转换失败
    ^^^^^^^^^^^^^^^^^

    如果环境变量的值无法转换为配置文件中的原始类型（比如把字符串转换为整数失败），
    函数会记录警告日志，但会保持配置文件中的原值，不会中断程序运行。

    Returns:
        dict: 加载后的配置字典，包含所有配置项

    Raises:
        FileNotFoundError: 如果配置文件不存在或无法读取

    Example:
        >>> config = load_config()
        >>> print(config['MONGO_URI'])
        '127.0.0.1:27017'
    """
    global config
    
    # 获取当前文件所在目录，用于定位配置文件
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'config.yaml')
    
    # 从配置文件加载基础配置
    # 使用 UTF-8 编码确保中文配置正确读取
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config = yaml.safe_load(config_file)
            logger.info(f"从配置文件加载配置: {config_path}")
    except Exception as e:
        error_msg = f"从 {config_path} 加载配置失败: {str(e)}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # 从环境变量更新配置
    # 环境变量的优先级高于配置文件，可以覆盖配置文件中的值
    for key in config.keys():
        env_value = os.environ.get(key)
        if env_value is not None:
            # 尝试将环境变量的值转换为与配置文件中相同的类型
            # 这样可以保持配置值的数据类型一致性
            try:
                original_type = type(config[key])
                if original_type == bool:
                    # 布尔类型的特殊处理：支持多种表示方式
                    config[key] = env_value.lower() in ('true', '1', 'yes')
                else:
                    # 其他类型直接转换
                    config[key] = original_type(env_value)
                logger.info(f"从环境变量更新配置: {key}")
            except ValueError:
                # 类型转换失败时，保持配置文件中的原值，并记录警告
                logger.warning(f"环境变量 {key} 的值类型转换失败，保持原值")
                continue
    
    return config

def get_config():
    """获取配置对象，如果配置未加载则先加载配置

    这个函数就像一个"配置获取器"，它会确保配置已经加载，
    如果还没有加载，就自动加载一次，然后返回配置对象。

    为什么需要这个函数？
    --------------------

    在项目中，很多地方都需要访问配置信息，比如：
    - 数据库连接需要 `MONGO_URI`
    - 日志配置需要 `LOG_LEVEL`
    - API 密钥需要 `LLM_API_KEY`

    如果每次都直接访问全局变量 `config`，可能会遇到配置还没加载的情况。
    这个函数提供了安全的访问方式，确保配置在使用前已经加载完成。

    工作原理（简单理解）
    ------------------

    就像去图书馆借书：

    1. **检查书是否在架子上**：检查全局 `config` 变量是否为 `None`
       （就像检查书是否已经在书架上）

    2. **如果没有就去取**：如果 `config` 为 `None`，就调用 `load_config()` 加载配置
       （就像如果书不在书架上，就去仓库取）

    3. **返回书**：返回配置对象
       （就像把书给借阅者）

    实际使用场景
    -----------

    在项目的任何地方需要访问配置时使用：

    ```python
    from panda_common.config import get_config

    config = get_config()
    mongo_uri = config['MONGO_URI']
    log_level = config['LOG_LEVEL']
    ```

    注意事项
    --------

    - 第一次调用时会自动加载配置
    - 配置加载后会被缓存，后续调用直接返回缓存的配置
    - 如果需要重新加载配置，可以手动调用 `load_config()`

    Returns:
        dict: 配置信息字典，包含所有配置项

    Example:
        >>> config = get_config()
        >>> print(config['MONGO_DB'])
        'panda'
    """
    global config
    # 懒加载模式：只有在第一次使用时才加载配置
    # 这样可以避免在模块导入时就加载配置，提高启动速度
    if config is None:
        config = load_config()
    return config

# 初始加载配置
# 在模块导入时自动加载配置，这样可以在应用启动时就完成配置加载
# 如果加载失败，不抛出异常，而是记录错误日志
# 这样可以让应用继续启动，配置会在实际使用时通过 get_config() 再次尝试加载
try:
    config = load_config()
    logger.info(f"初始化配置成功: {config}")
except Exception as e:
    logger.error(f"初始化配置失败: {str(e)}")
    # 不在初始化时抛出异常，留到实际使用时再处理
    # 这样可以避免因为配置问题导致整个应用无法启动
