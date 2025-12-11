"""
数据库处理模块

本模块提供了统一的数据库访问接口，就像一个"数据库管家"，
它会管理 MongoDB 数据库连接，并提供各种数据库操作方法，
让整个项目可以方便地访问和操作数据库，而不需要关心底层的连接细节。

核心概念
--------

- **DatabaseHandler**：数据库处理器类，使用单例模式确保整个应用只有一个数据库连接实例
- **单例模式**：确保无论创建多少次 DatabaseHandler 实例，都返回同一个对象
- **MongoDB 连接**：支持单节点和副本集两种连接模式

使用方式
--------

1. 创建 DatabaseHandler 实例（会自动使用单例模式）
2. 使用各种方法进行数据库操作（插入、查询、更新、删除等）

工作原理
--------

就像图书馆的管理系统：

1. **单例模式**：无论你创建多少次 DatabaseHandler，都得到同一个实例
   （就像图书馆只有一个管理系统，不会重复创建）

2. **连接管理**：在第一次创建时建立数据库连接，后续都复用这个连接
   （就像图书馆只开一次门，之后所有人都用这个门）

3. **操作封装**：提供统一的接口进行数据库操作，隐藏底层细节
   （就像通过图书管理员借书，不需要自己去找书）

注意事项
--------

- 密码中的特殊字符会自动进行 URL 编码，避免认证问题
- 支持单节点和副本集两种 MongoDB 部署模式
- 连接失败时会抛出异常，需要确保数据库服务正常运行
- 所有操作都使用统一的连接，确保连接池的高效利用
"""

import pymongo
import urllib.parse
import os
import logging
from typing import Optional, Dict, List

# 设置日志
logger = logging.getLogger(__name__)


class DatabaseHandler:
    """数据库处理器类：统一的数据库访问接口

    这个类就像一个"数据库管家"，它使用单例模式确保整个应用只有一个数据库连接，
    并提供各种数据库操作方法，让整个项目可以方便地访问和操作 MongoDB 数据库。

    为什么需要这个类？
    --------------------

    在项目中，很多地方都需要访问数据库：
    - 读取市场数据
    - 存储因子数据
    - 查询用户信息
    - 记录分析日志

    如果每个地方都自己创建数据库连接，会导致：
    - 连接数过多，浪费资源
    - 连接管理混乱，难以维护
    - 配置分散，容易出错

    这个类通过单例模式统一管理数据库连接，解决了这些问题。

    工作原理（简单理解）
    ------------------

    就像图书馆的管理系统：

    1. **单例模式**：无论创建多少次 DatabaseHandler，都返回同一个实例
       （就像图书馆只有一个管理系统，不会重复创建）

    2. **懒加载连接**：只有在第一次使用时才建立数据库连接
       （就像图书馆在第一次有人借书时才开门）

    3. **连接复用**：后续所有操作都使用同一个连接
       （就像所有人都通过同一个门进出图书馆）

    4. **操作封装**：提供统一的接口进行数据库操作
       （就像通过图书管理员借书，不需要自己去找书）

    实际使用场景
    -----------

    在项目的任何地方需要访问数据库时使用：

    ```python
    from panda_common.config import get_config
    from panda_common.handlers.database_handler import DatabaseHandler

    config = get_config()
    db_handler = DatabaseHandler(config)

    # 查询数据
    results = db_handler.mongo_find("panda", "stock_market", {"date": "20240101"})

    # 插入数据
    db_handler.mongo_insert("panda", "user_factors", {"user_id": "123", "factor_name": "test"})
    ```

    可能遇到的问题
    ------------

    连接失败
    ^^^^^^^

    如果 MongoDB 服务未启动或配置错误，初始化时会抛出异常。
    确保 MongoDB 服务正常运行，并且配置信息正确。

    密码特殊字符
    ^^^^^^^^^^^

    如果密码中包含特殊字符（如 @、#、% 等），会自动进行 URL 编码。
    这是为了避免在连接字符串中出现特殊字符导致的认证问题。

    Attributes:
        mongo_client: MongoDB 客户端连接对象
        _instance: 单例模式的实例变量（类变量）
        initialized: 初始化标志，防止重复初始化
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        """创建 DatabaseHandler 实例（单例模式实现）

        这个方法实现了单例模式，确保无论创建多少次 DatabaseHandler，
        都返回同一个实例。

        为什么需要单例模式？
        --------------------

        数据库连接是昂贵的资源，如果每次需要访问数据库时都创建新连接，
        会导致连接数过多，浪费资源，甚至可能导致数据库连接池耗尽。

        单例模式确保整个应用只有一个数据库连接实例，所有操作都复用这个连接。

        工作原理
        --------

        1. 检查类变量 `_instance` 是否已存在
        2. 如果不存在，创建新实例并保存到 `_instance`
        3. 如果已存在，直接返回已有的实例

        Returns:
            DatabaseHandler: 数据库处理器实例（单例）
        """
        if not cls._instance:
            cls._instance = super(DatabaseHandler, cls).__new__(cls)
        return cls._instance

    def __init__(self, config):
        """初始化数据库处理器

        这个方法负责建立 MongoDB 数据库连接。由于使用了单例模式，
        只有在第一次创建实例时才会执行初始化，后续创建会跳过初始化。

        为什么需要防止重复初始化？
        -------------------------

        由于单例模式，`__new__` 方法会返回同一个实例，但 Python 会为同一个实例
        多次调用 `__init__` 方法。如果不检查，会导致重复建立连接，浪费资源。

        工作原理
        --------

        1. 检查是否已经初始化（通过 `initialized` 属性）
        2. 如果未初始化，执行以下步骤：
           - URL 编码密码（处理特殊字符）
           - 构建连接字符串
           - 根据配置类型（单节点/副本集）创建连接
           - 测试连接是否成功
        3. 如果已初始化，直接跳过

        Args:
            config: 配置字典，必须包含以下键：
                - MONGO_USER: MongoDB 用户名
                - MONGO_PASSWORD: MongoDB 密码
                - MONGO_URI: MongoDB 连接地址
                - MONGO_AUTH_DB: 认证数据库名称
                - MONGO_TYPE: 连接类型（'single' 或 'replica_set'）
                - MONGO_REPLICA_SET: 副本集名称（如果使用副本集）

        Raises:
            Exception: 如果数据库连接失败，会抛出异常

        Note:
            - 密码中的特殊字符会自动进行 URL 编码
            - 支持单节点和副本集两种连接模式
            - 连接成功后会打印连接信息（密码会被隐藏）
        """
        if not hasattr(self, 'initialized'):  # Prevent re-initialization

            # URL 编码密码，避免特殊字符导致的认证问题
            # 例如：如果密码是 "p@ss#word"，会被编码为 "p%40ss%23word"
            encoded_password = urllib.parse.quote_plus(config["MONGO_PASSWORD"])

            # 构建 MongoDB 连接字符串
            # 格式：mongodb://用户名:密码@地址/认证数据库
            MONGO_URI = f'mongodb://{config["MONGO_USER"]}:{encoded_password}@{config["MONGO_URI"]}/{config["MONGO_AUTH_DB"]}'
            
            # 根据配置的连接类型创建不同的连接
            if (config['MONGO_TYPE'] == 'single'):
                # 单节点模式：直接连接到单个 MongoDB 服务器
                self.mongo_client = pymongo.MongoClient(
                    MONGO_URI,
                    readPreference='secondaryPreferred',  # 优先从从节点读取，提高读取性能
                    w='majority',  # 写关注级别：确保写入被大多数节点确认
                    retryWrites=True,  # 自动重试写操作，提高可靠性
                    socketTimeoutMS=30000,  # Socket 超时时间：30秒
                    connectTimeoutMS=20000,  # 连接超时时间：20秒
                    serverSelectionTimeoutMS=30000,  # 服务器选择超时时间：30秒
                    authSource=config["MONGO_AUTH_DB"],  # 明确指定认证数据库
                )
            elif (config['MONGO_TYPE'] == 'replica_set'):
                # 副本集模式：连接到 MongoDB 副本集
                # 在连接字符串中添加副本集名称
                MONGO_URI += f'?replicaSet={config["MONGO_REPLICA_SET"]}'
                self.mongo_client = pymongo.MongoClient(
                    MONGO_URI,
                    readPreference='secondaryPreferred',  # 优先从从节点读取，分担主节点压力
                    w='majority',  # 写关注级别：确保写入被大多数节点确认，保证数据一致性
                    retryWrites=True,  # 自动重试写操作，提高可靠性
                    socketTimeoutMS=30000,  # Socket 超时时间：30秒
                    connectTimeoutMS=20000,  # 连接超时时间：20秒
                    serverSelectionTimeoutMS=30000,  # 服务器选择超时时间：30秒
                    authSource=config["MONGO_AUTH_DB"],  # 明确指定认证数据库
                )

            # 打印连接信息（隐藏密码，保护安全）
            # 将连接字符串中的密码替换为 "****"，避免在日志中暴露密码
            masked_uri = MONGO_URI
            masked_uri = masked_uri.replace(urllib.parse.quote_plus(config["MONGO_PASSWORD"]), "****")
            
            # 测试连接是否成功
            # 使用 ping 命令测试数据库连接，这是最轻量级的测试方式
            try:
                # 发送 ping 命令到数据库，如果连接成功会返回响应
                self.mongo_client.admin.command('ping')
                print(f"Connecting to MongoDB: {masked_uri}")
            except Exception as e:
                # 连接失败时打印错误信息并抛出异常
                print(f"MongoDB connection failed: {e}")
                raise
            
            # 以下代码为预留接口，用于未来支持 MySQL 和 Redis
            # 目前项目主要使用 MongoDB，如需启用请取消注释并配置相应参数
            # self.mysql_conn = mysql.connector.connect(
            #     host=config.MYSQL_HOST,
            #     user=config.MYSQL_USER,
            #     password=config.MYSQL_PASSWORD,
            #     database=config.MYSQL_DATABASE
            # )
            # self.redis_client = redis.StrictRedis(
            #     host=config.REDIS_HOST,
            #     port=config.REDIS_PORT,
            #     password=config.REDIS_PASSWORD,
            #     decode_responses=True
            # )
            
            # 标记初始化完成，防止重复初始化
            self.initialized = True

    def mongo_insert(self, db_name, collection_name, document):
        """向 MongoDB 集合中插入单个文档

        这个函数就像一个"数据录入员"，它会将一条数据插入到指定的数据库集合中。

        为什么需要这个函数？
        --------------------

        在项目中，经常需要将数据保存到数据库，比如：
        - 保存用户创建的因子
        - 记录分析任务的结果
        - 存储配置信息

        这个函数提供了简单的方式来插入数据，而不需要直接操作 MongoDB 客户端。

        工作原理
        --------

        1. 获取指定的集合对象
        2. 调用 `insert_one()` 方法插入文档
        3. 返回插入文档的 ID

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "user_factors"
            document: 要插入的文档（字典格式）

        Returns:
            ObjectId: 插入文档的 ID

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> doc_id = db_handler.mongo_insert("panda", "user_factors", {
            ...     "user_id": "123",
            ...     "factor_name": "test_factor"
            ... })
            >>> print(doc_id)
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.insert_one(document).inserted_id

    def mongo_find(self, db_name, collection_name, query, projection=None, hint=None, sort=None):
        """从 MongoDB 集合中查询多个文档

        这个函数就像一个"数据查询员"，它会根据查询条件从数据库中查找匹配的文档。

        为什么需要这个函数？
        --------------------

        在项目中，经常需要从数据库查询数据，比如：
        - 查询某个日期范围内的市场数据
        - 查找用户创建的所有因子
        - 获取分析任务的结果

        这个函数提供了灵活的查询方式，支持条件查询、字段投影、排序等功能。

        工作原理
        --------

        就像在图书馆找书：

        1. **指定书架**：获取指定的数据库和集合（就像确定在哪个书架找书）
        2. **设置条件**：使用查询条件筛选文档（就像根据书名、作者等条件找书）
        3. **选择字段**：使用投影选择返回的字段（就像只拿需要的书页）
        4. **使用索引**：使用 hint 指定索引（就像使用图书索引快速定位）
        5. **排序结果**：对结果进行排序（就像按字母顺序排列找到的书）

        实际使用场景
        -----------

        查询市场数据：

        ```python
        # 查询 2024年1月1日 的所有股票数据
        results = db_handler.mongo_find(
            "panda",
            "stock_market",
            {"date": "20240101"},
            projection={"symbol": 1, "close": 1, "volume": 1}
        )
        ```

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "stock_market"
            query: 查询条件字典，如 {"date": "20240101", "symbol": "000001"}
            projection: 字段投影字典，指定返回哪些字段，如 {"symbol": 1, "close": 1}
            hint: 索引提示，指定使用哪个索引，如 [("date", 1), ("symbol", 1)]
            sort: 排序规则，如 [("date", -1)] 表示按日期降序

        Returns:
            List[Dict]: 查询结果列表，每个元素是一个文档字典

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> results = db_handler.mongo_find(
            ...     "panda",
            ...     "stock_market",
            ...     {"date": "20240101"},
            ...     sort=[("symbol", 1)]
            ... )
            >>> print(len(results))
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        cursor = collection.find(query, projection)
        # 如果指定了索引提示，使用指定的索引
        if hint:
            cursor = cursor.hint(hint)
        # 如果指定了排序规则，对结果进行排序
        if sort:
            cursor = cursor.sort(sort)
        return list(cursor)

    def mongo_update(self, db_name, collection_name, query, update):
        """更新 MongoDB 集合中的多个文档

        这个函数就像一个"数据修改员"，它会根据查询条件找到匹配的文档并更新它们。

        为什么需要这个函数？
        --------------------

        在项目中，经常需要更新数据库中的数据，比如：
        - 更新任务的状态
        - 修改用户的配置
        - 更新因子的计算结果

        这个函数提供了批量更新的能力，可以一次性更新多个匹配的文档。

        工作原理
        --------

        1. 获取指定的集合对象
        2. 使用查询条件找到匹配的文档
        3. 使用 `$set` 操作符更新文档的字段
        4. 返回被更新的文档数量

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "tasks"
            query: 查询条件字典，用于匹配要更新的文档
            update: 更新内容字典，包含要更新的字段和值

        Returns:
            int: 被更新的文档数量

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> count = db_handler.mongo_update(
            ...     "panda",
            ...     "tasks",
            ...     {"task_id": "123"},
            ...     {"status": "completed", "updated_at": "2024-01-01"}
            ... )
            >>> print(f"更新了 {count} 条记录")
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        # 使用 $set 操作符更新文档，只更新指定的字段，不影响其他字段
        return collection.update_many(query, {'$set': update}).modified_count


    def mongo_delete(self, db_name, collection_name, query):
        """从 MongoDB 集合中删除多个文档

        这个函数就像一个"数据清理员"，它会根据查询条件找到匹配的文档并删除它们。

        为什么需要这个函数？
        --------------------

        在项目中，有时需要删除数据库中的数据，比如：
        - 删除过期的临时数据
        - 清理测试数据
        - 删除用户不需要的因子

        这个函数提供了批量删除的能力，可以一次性删除多个匹配的文档。

        工作原理
        --------

        1. 获取指定的集合对象
        2. 使用查询条件找到匹配的文档
        3. 调用 `delete_many()` 方法删除所有匹配的文档
        4. 返回被删除的文档数量

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "temp_data"
            query: 查询条件字典，用于匹配要删除的文档

        Returns:
            int: 被删除的文档数量

        Warning:
            删除操作不可逆，请谨慎使用。建议在删除前先备份数据。

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> count = db_handler.mongo_delete(
            ...     "panda",
            ...     "temp_data",
            ...     {"created_at": {"$lt": "20240101"}}
            ... )
            >>> print(f"删除了 {count} 条记录")
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.delete_many(query).deleted_count

    def get_mongo_collection(self, db_name, collection_name):
        """获取 MongoDB 集合对象

        这个函数就像一个"书架定位器"，它会根据数据库名和集合名返回对应的集合对象。

        为什么需要这个函数？
        --------------------

        在 MongoDB 中，所有的数据操作都是通过集合对象进行的。
        这个函数提供了统一的方式来获取集合对象，隐藏了底层的数据结构。

        工作原理
        --------

        1. 从 MongoDB 客户端获取指定的数据库对象
        2. 从数据库对象获取指定的集合对象
        3. 返回集合对象，用于后续的数据操作

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "stock_market"

        Returns:
            Collection: MongoDB 集合对象，可以用于各种数据操作

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> collection = db_handler.get_mongo_collection("panda", "stock_market")
            >>> # 现在可以直接使用 collection 进行各种操作
        """
        return self.mongo_client[db_name][collection_name]

    # def mysql_query(self, query, params=None):
    #     cursor = self.mysql_conn.cursor()
    #     cursor.execute(query, params)
    #     return cursor.fetchall()

    # def redis_set(self, key, value):
    #     self.redis_client.set(key, value)

    # def redis_get(self, key):
    #     return self.redis_client.get(key)

    # def mysql_insert(self, table, data):
    #     cursor = self.mysql_conn.cursor()
    #     placeholders = ', '.join(['%s'] * len(data))
    #     columns = ', '.join(data.keys())
    #     sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    #     cursor.execute(sql, list(data.values()))
    #     self.mysql_conn.commit()
    #     return cursor.lastrowid

    # def mysql_update(self, table, data, condition):
    #     cursor = self.mysql_conn.cursor()
    #     set_clause = ', '.join([f"{key} = %s" for key in data.keys()])
    #     sql = f"UPDATE {table} SET {set_clause} WHERE {condition}"
    #     cursor.execute(sql, list(data.values()))
    #     self.mysql_conn.commit()
    #     return cursor.rowcount

    # def mysql_delete(self, table, condition):
    #     cursor = self.mysql_conn.cursor()
    #     sql = f"DELETE FROM {table} WHERE {condition}"
    #     cursor.execute(sql)
    #     self.mysql_conn.commit()
    #     return cursor.rowcount

    def mongo_insert_many(self, db_name, collection_name, documents):
        """向 MongoDB 集合中批量插入多个文档

        这个函数就像一个"批量数据录入员"，它会将多条数据一次性插入到数据库中。

        为什么需要这个函数？
        --------------------

        当需要插入大量数据时，如果一条一条插入会很慢。
        这个函数支持批量插入，可以一次性插入多条数据，大大提高插入效率。

        工作原理
        --------

        1. 获取指定的集合对象
        2. 调用 `insert_many()` 方法批量插入文档
        3. 返回所有插入文档的 ID 列表

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "stock_market"
            documents: 要插入的文档列表，每个元素是一个字典

        Returns:
            List[ObjectId]: 插入文档的 ID 列表

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> docs = [
            ...     {"symbol": "000001", "date": "20240101", "close": 10.5},
            ...     {"symbol": "000002", "date": "20240101", "close": 20.3}
            ... ]
            >>> ids = db_handler.mongo_insert_many("panda", "stock_market", docs)
            >>> print(f"插入了 {len(ids)} 条记录")
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.insert_many(documents).inserted_ids

    def mongo_aggregate(self, db_name, collection_name, aggregation_pipeline):
        """在 MongoDB 集合上执行聚合操作

        这个函数就像一个"数据分析师"，它可以对数据库中的数据进行复杂的聚合分析，
        比如分组统计、数据转换、数据筛选等。

        为什么需要这个函数？
        --------------------

        在项目中，经常需要进行复杂的数据分析，比如：
        - 统计每个日期的数据条数
        - 计算某个字段的平均值
        - 按条件分组统计

        这些操作如果使用普通的查询会很复杂，聚合操作可以一次性完成。

        工作原理
        --------

        聚合操作就像工厂的生产流水线：

        1. **输入数据**：从集合中读取原始数据
        2. **流水线处理**：按照聚合管道中的步骤逐步处理数据
           - `$match`：筛选数据（就像筛选原材料）
           - `$group`：分组统计（就像按类别分组）
           - `$project`：字段投影（就像选择需要的字段）
           - `$sort`：排序（就像按顺序排列）
        3. **输出结果**：返回处理后的数据列表

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "stock_market"
            aggregation_pipeline: 聚合管道列表，每个元素是一个聚合操作字典

        Returns:
            List[Dict]: 聚合操作的结果列表

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> pipeline = [
            ...     {"$match": {"date": {"$gte": "20240101"}}},
            ...     {"$group": {"_id": "$date", "count": {"$sum": 1}}},
            ...     {"$sort": {"_id": 1}}
            ... ]
            >>> results = db_handler.mongo_aggregate("panda", "stock_market", pipeline)
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        return list(collection.aggregate(aggregation_pipeline)) 
    
    def get_distinct_values(self, db_name, collection_name, field):
        """获取集合中某个字段的所有不重复值

        这个函数就像一个"数据去重器"，它会返回集合中某个字段的所有唯一值。

        为什么需要这个函数？
        --------------------

        在项目中，经常需要获取某个字段的所有可能值，比如：
        - 获取所有股票代码
        - 获取所有日期
        - 获取所有用户 ID

        这个函数可以快速获取这些唯一值，而不需要查询所有文档然后手动去重。

        工作原理
        --------

        1. 获取指定的集合对象
        2. 调用 `distinct()` 方法获取字段的所有唯一值
        3. 返回唯一值列表

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "stock_market"
            field: 字段名称，如 "symbol"

        Returns:
            List: 字段的所有唯一值列表

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> symbols = db_handler.get_distinct_values("panda", "stock_market", "symbol")
            >>> print(f"共有 {len(symbols)} 个不同的股票代码")
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.distinct(field)

    def mongo_find_one(self, db_name, collection_name, query, hint=None):
        """从 MongoDB 集合中查询单个文档

        这个函数就像一个"精确查找器"，它会根据查询条件查找第一个匹配的文档。

        为什么需要这个函数？
        --------------------

        当只需要查找一条记录时，使用 `mongo_find()` 会返回列表，需要手动取第一个元素。
        这个函数直接返回单个文档，使用更方便，性能也更好（找到第一个就停止）。

        工作原理
        --------

        1. 获取指定的集合对象
        2. 使用查询条件查找第一个匹配的文档
        3. 如果指定了索引提示，使用指定的索引
        4. 返回找到的文档，如果没找到返回 None

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "user_factors"
            query: 查询条件字典，如 {"_id": ObjectId("...")}
            hint: 索引提示，指定使用哪个索引，如 [("user_id", 1)]

        Returns:
            Optional[Dict]: 找到的文档字典，如果没找到返回 None

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> doc = db_handler.mongo_find_one(
            ...     "panda",
            ...     "user_factors",
            ...     {"user_id": "123", "factor_name": "test"}
            ... )
            >>> if doc:
            ...     print(doc)
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        # 如果指定了索引提示，使用指定的索引
        if hint:
            return collection.find_one(query, hint=hint)
        return collection.find_one(query)

    def find_documents(self,
                       db_name: str,
                       collection_name: str,
                       filter_dict: Optional[Dict] = None,
                       projection: Optional[Dict] = None,
                       limit: Optional[int] = None,
                       sort: Optional[List] = None) -> List[Dict]:
        """查询文档（带错误处理的增强版查询方法）

        这个函数是 `mongo_find()` 的增强版，它提供了更完善的错误处理机制，
        即使查询失败也不会抛出异常，而是返回空列表并记录错误日志。

        为什么需要这个函数？
        --------------------

        在某些场景下，我们希望查询失败时不要中断程序运行，而是优雅地处理错误。
        比如在数据统计时，如果某个集合查询失败，可以跳过继续处理其他集合。

        工作原理
        --------

        1. 获取指定的集合对象
        2. 使用查询条件查找文档
        3. 如果指定了排序，对结果进行排序
        4. 如果指定了限制，限制返回数量
        5. 如果发生异常，记录错误日志并返回空列表

        Args:
            db_name: 数据库名称，如 "panda"
            collection_name: 集合名称，如 "stock_market"
            filter_dict: 查询条件字典，如果为 None 则查询所有文档
            projection: 字段投影字典，指定返回哪些字段
            limit: 限制返回数量，如果为 None 则返回所有匹配的文档
            sort: 排序规则列表，如 [("date", -1)] 表示按日期降序

        Returns:
            List[Dict]: 查询结果列表，如果查询失败返回空列表

        Example:
            >>> db_handler = DatabaseHandler(config)
            >>> results = db_handler.find_documents(
            ...     "panda",
            ...     "stock_market",
            ...     filter_dict={"date": "20240101"},
            ...     limit=100,
            ...     sort=[("symbol", 1)]
            ... )
            >>> print(f"查询到 {len(results)} 条记录")
        """
        try:
            collection = self.get_mongo_collection(db_name, collection_name)

            # 构建查询游标，如果 filter_dict 为 None 则查询所有文档
            cursor = collection.find(
                filter_dict or {},
                projection
            )

            # 如果指定了排序规则，对结果进行排序
            if sort:
                cursor = cursor.sort(sort)

            # 如果指定了限制数量，限制返回的文档数
            if limit:
                cursor = cursor.limit(limit)

            # 将游标转换为列表
            results = list(cursor)
            logger.info(f"从集合 {collection_name} 查询到 {len(results)} 条记录")
            return results

        except Exception as e:
            # 查询失败时记录错误日志，但不抛出异常，返回空列表
            # 这样可以避免因为单个查询失败导致整个程序崩溃
            logger.error(f"查询集合 {collection_name} 失败: {e}")
            return []