"""
用户因子模型模块

本模块定义了用户因子的数据模型，就像一个"因子档案模板"，
它提供了标准化的因子数据结构，确保用户创建的因子能够正确存储和管理。

核心概念
--------

- **UserFactor**：用户因子类，包含因子的所有信息

使用方式
--------

1. 创建 `UserFactor` 实例，定义用户因子
2. 使用 `to_dict()` 方法转换为字典，保存到数据库
3. 使用 `from_dict()` 方法从字典创建实例，从数据库读取

工作原理
--------

就像管理因子档案：

1. **创建档案**：创建 `UserFactor` 实例（就像创建因子档案）
2. **保存档案**：转换为字典保存到数据库（就像归档）
3. **读取档案**：从字典创建实例（就像查阅档案）
"""

from datetime import datetime
from typing import Dict, Optional


class UserFactor:
    """用户因子类：管理用户创建的因子信息

    这个类就像一个"因子档案"，它包含了用户创建因子的所有信息，
    包括因子名称、代码、状态、进度等，让你可以方便地管理和追踪因子。

    为什么需要这个类？
    --------------------

    在项目中，用户会创建很多因子，每个因子都有：
    - 基本信息（名称、代码、类型）
    - 状态信息（是否持久化、计算状态、进度）
    - 元数据（创建时间、更新时间、描述）

    如果这些信息分散管理，会导致：
    - 数据不一致
    - 难以追踪
    - 代码复杂

    这个类将所有信息集中管理，解决了这些问题。

    工作原理（简单理解）
    ------------------

    就像管理因子档案：

    1. **创建档案**：初始化时设置所有信息（就像填写档案表）
    2. **保存档案**：使用 `to_dict()` 转换为字典（就像归档）
    3. **读取档案**：使用 `from_dict()` 从字典创建（就像查阅档案）
    4. **更新档案**：修改属性后更新 `gmt_updated`（就像更新档案）

    实际使用场景
    -----------

    创建和保存用户因子：

    ```python
    # 创建因子
    factor = UserFactor(
        user_id="user123",
        name="我的因子",
        factor_name="momentum_factor",
        type="python",
        is_persistent=True,
        code="class MyFactor(Factor): ...",
        status=0,
        progress=0
    )

    # 保存到数据库
    db_handler.mongo_insert("panda", "user_factors", factor.to_dict())

    # 从数据库读取
    data = db_handler.mongo_find_one("panda", "user_factors", {"_id": factor_id})
    factor = UserFactor.from_dict(data)
    ```

    Attributes:
        user_id: 用户 ID，标识因子属于哪个用户
        name: 因子显示名称，用于前端展示
        factor_name: 因子名称（代码中的名称），用于标识因子
        type: 因子类型，如 "python"（Python 类）或 "formula"（公式）
        is_persistent: 是否持久化，True 表示计算结果会保存到数据库
        code: 因子代码，Python 类代码或公式字符串
        status: 因子状态，0=待计算，1=计算中，2=已完成，-1=失败
        progress: 计算进度，0-100 的整数
        describe: 因子描述，用户对因子的说明
        params: 因子参数字典，包含分析参数等
        gmt_created: 创建时间
        gmt_updated: 更新时间
    """
    
    def __init__(
        self,
        user_id: str,
        name: str,
        factor_name: str,
        type: str,
        is_persistent: bool,
        code: str,
        status: int,
        progress: int,
        describe: str = "",
        params: Optional[Dict] = None,
        gmt_created: Optional[datetime] = None,
        gmt_updated: Optional[datetime] = None
    ):
        """初始化用户因子

        Args:
            user_id: 用户 ID
            name: 因子显示名称
            factor_name: 因子名称（代码中的名称）
            type: 因子类型，"python" 或 "formula"
            is_persistent: 是否持久化
            code: 因子代码
            status: 因子状态，0=待计算，1=计算中，2=已完成，-1=失败
            progress: 计算进度，0-100
            describe: 因子描述
            params: 因子参数字典
            gmt_created: 创建时间，如果为 None 则使用当前时间
            gmt_updated: 更新时间，如果为 None 则使用当前时间
        """
        self.user_id = user_id
        self.name = name
        self.factor_name = factor_name
        self.type = type
        self.is_persistent = is_persistent
        self.code = code
        self.status = status
        self.progress = progress
        self.describe = describe
        self.params = params or {}
        # 如果未提供时间，使用当前时间
        self.gmt_created = gmt_created or datetime.now()
        self.gmt_updated = gmt_updated or datetime.now()

    def to_dict(self) -> Dict:
        """将因子对象转换为字典

        这个函数就像一个"归档员"，它会将因子对象的所有信息转换为字典格式，
        方便保存到数据库或序列化为 JSON。

        为什么需要这个函数？
        --------------------

        MongoDB 只能存储字典格式的数据，不能直接存储 Python 对象。
        这个函数提供了将对象转换为字典的能力。

        工作原理
        --------

        1. 收集对象的所有属性
        2. 构建字典，键为属性名，值为属性值
        3. 返回字典

        Returns:
            Dict: 包含因子所有信息的字典

        Example:
            >>> factor = UserFactor(...)
            >>> data = factor.to_dict()
            >>> db_handler.mongo_insert("panda", "user_factors", data)
        """
        return {
            "user_id": self.user_id,
            "name": self.name,
            "factor_name": self.factor_name,
            "type": self.type,
            "is_persistent": self.is_persistent,
            "code": self.code,
            "status": self.status,
            "progress": self.progress,
            "describe": self.describe,
            "params": self.params,
            "gmt_created": self.gmt_created,
            "gmt_updated": self.gmt_updated
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "UserFactor":
        """从字典创建因子对象

        这个函数就像一个"档案查阅员"，它会从字典数据中创建因子对象，
        方便从数据库读取数据后转换为对象。

        为什么需要这个函数？
        --------------------

        从数据库读取的数据是字典格式，需要转换为对象才能方便使用。
        这个函数提供了从字典创建对象的能力。

        工作原理
        --------

        1. 从字典中提取各个字段的值
        2. 使用提取的值创建因子对象
        3. 返回因子对象

        Args:
            data: 包含因子信息的字典

        Returns:
            UserFactor: 因子对象实例

        Example:
            >>> data = db_handler.mongo_find_one("panda", "user_factors", {"_id": factor_id})
            >>> factor = UserFactor.from_dict(data)
        """
        return cls(
            user_id=data["user_id"],
            name=data["name"],
            factor_name=data["factor_name"],
            type=data["type"],
            is_persistent=data["is_persistent"],
            code=data["code"],
            status=data["status"],
            # progress 字段可能不存在，使用默认值 0
            progress=data.get("progress", 0),
            describe=data.get("describe", ""),
            params=data.get("params", {}),
            gmt_created=data.get("gmt_created"),
            gmt_updated=data.get("gmt_updated")
        )
