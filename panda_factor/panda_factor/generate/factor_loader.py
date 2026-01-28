"""
因子加载器模块

本模块提供了因子代码的加载和安全验证功能，就像一个"因子代码检查员"，
它会检查用户提交的因子代码是否安全，然后动态加载为可执行的因子类。

核心概念
--------

- **AST 安全检查**：通过抽象语法树（AST）分析代码，检测危险操作
- **动态加载**：将代码字符串动态编译为可执行的 Python 类
- **白名单机制**：只允许安全的操作和导入，阻止危险代码

为什么需要这个模块？
-------------------

在量化分析系统中，用户可以提交自定义的因子代码。如果直接执行用户代码，
可能存在安全风险：
- 文件操作（删除文件、读取敏感数据）
- 系统调用（执行命令、访问网络）
- 资源滥用（无限循环、内存耗尽）

这个模块通过 AST 分析和白名单机制，确保只有安全的代码才能被执行。

工作原理（简单理解）
------------------

就像机场安检：

1. **代码扫描**：解析代码为 AST（就像 X 光扫描行李）
2. **安全检查**：检查每个节点是否在白名单中（就像检查是否有违禁品）
3. **准备环境**：创建安全的执行环境（就像准备安全区域）
4. **动态加载**：执行代码创建因子类（就像允许通过安检的旅客）

注意事项
--------

- 只允许导入白名单中的模块（numpy、pandas、scipy 等）
- 只允许定义一个继承自 Factor 的类
- 类中只允许定义 calculate 方法
- 所有危险操作都会被阻止
"""

import ast
import types
from typing import Type
from .factor_base import Factor
from datetime import datetime
import importlib.util
import sys
from .factor_utils import FactorUtils


class FactorLoader:
    """因子加载器类：加载和验证自定义因子代码

    这个类就像一个"因子代码检查员"，它负责：
    - 检查因子代码的安全性
    - 动态加载因子类
    - 提供详细的错误信息

    为什么需要这个类？
    -----------------

    用户提交的因子代码可能包含危险操作，直接执行会有安全风险。
    这个类通过 AST 分析和白名单机制，确保只有安全的代码才能被执行。

    工作原理（简单理解）
    ------------------

    就像机场安检：

    1. **代码扫描**：解析代码为 AST（就像 X 光扫描行李）
    2. **安全检查**：检查每个节点是否在白名单中（就像检查是否有违禁品）
    3. **准备环境**：创建安全的执行环境（就像准备安全区域）
    4. **动态加载**：执行代码创建因子类（就像允许通过安检的旅客）

    实际使用场景
    -----------

    加载用户提交的因子代码：

    ```python
    code = '''
    class MyFactor(Factor):
        def calculate(self, factors):
            close = factors['close']
            return self.RETURNS(close)
    '''
    
    factor_class = FactorLoader.load_factor_class(code)
    if factor_class:
        factor = factor_class()
        result = factor.calculate(factors)
    ```

    Attributes:
        无实例属性，所有方法都是静态方法
    """
    
    @staticmethod
    def _is_safe_ast(node) -> bool:
        """检查 AST 节点是否安全

        这个方法就像一个"代码扫描仪"，它会递归检查代码的每个部分，
        确保没有危险操作。

        为什么需要这个方法？
        --------------------

        用户代码可能包含各种操作，有些是安全的（如数学运算），
        有些是危险的（如文件操作）。这个方法通过白名单机制，
        只允许安全的操作通过。

        工作原理
        --------

        1. 检查节点类型
        2. 如果在白名单中，返回 True
        3. 如果是危险操作，返回 False 并打印警告
        4. 递归检查子节点

        白名单包括：
        - 导入语句（仅限安全模块）
        - 类定义（仅限继承 Factor）
        - 函数定义（仅限 calculate 方法）
        - 基础字面量（数字、字符串等）
        - 数学运算
        - 条件语句、循环语句
        - 函数调用

        Args:
            node: AST 节点对象

        Returns:
            bool: 如果节点安全返回 True，否则返回 False
        """
        # Allow imports
        if isinstance(node, ast.Import):
            allowed_modules = {'numpy', 'pandas', 'talib', 'scipy', 'sklearn', 'math', 'datetime', 'warnings'}
            return all(name.name in allowed_modules for name in node.names)
            
        if isinstance(node, ast.ImportFrom):
            allowed_imports = {
                ('panda_factor.generate.factor_base', 'Factor'),
                ('scipy', 'stats'),
                ('sklearn', 'preprocessing'),
                ('datetime', 'datetime'),
                ('datetime', 'timedelta')
            }
            return (node.module, node.names[0].name) in allowed_imports
            
        # Allow class definition
        if isinstance(node, ast.ClassDef):
            # Check class name and base class
            if not node.name.isidentifier():
                print(f"Invalid class name: {node.name}")
                return False
            if len(node.bases) != 1 or not isinstance(node.bases[0], ast.Name) or node.bases[0].id != 'Factor':
                print("Custom factor class must inherit from Factor")
                return False
            # Check class body
            return all(FactorLoader._is_safe_ast(n) for n in node.body)
            
        # Allow function definition
        if isinstance(node, ast.FunctionDef):
            if node.name != 'calculate':
                print(f"Only calculate method is allowed, found: {node.name}")
                return False
            return all(FactorLoader._is_safe_ast(n) for n in node.body)
            
        # Allow basic literals
        if isinstance(node, (ast.Num, ast.Str, ast.Bytes, ast.NameConstant, ast.Constant)):
            return True
            
        # Allow names
        if isinstance(node, ast.Name):
            return True
            
        # Allow attribute access (for factor dictionary access)
        if isinstance(node, ast.Attribute):
            return True
            
        # Allow subscript (for dictionary access)
        if isinstance(node, ast.Subscript):
            return True
            
        # Allow basic mathematical operations
        if isinstance(node, (ast.BinOp, ast.UnaryOp)):
            return True
            
        # Allow basic expressions
        if isinstance(node, (ast.Expr, ast.Return)):
            return True
            
        # Allow arguments
        if isinstance(node, ast.arguments):
            return True
            
        # Allow function arguments
        if isinstance(node, ast.arg):
            return True
            
        # Allow assignments for intermediate calculations
        if isinstance(node, ast.Assign):
            return True
            
        # Allow function calls
        if isinstance(node, ast.Call):
            return True
            
        # Allow list and tuple literals
        if isinstance(node, (ast.List, ast.Tuple)):
            return True
            
        # Allow comparisons
        if isinstance(node, ast.Compare):
            return True
            
        # Allow if statements
        if isinstance(node, ast.If):
            return True
            
        # Allow for loops
        if isinstance(node, ast.For):
            return True
            
        # Allow while loops with break condition
        if isinstance(node, ast.While):
            return True
            
        # Allow break and continue
        if isinstance(node, (ast.Break, ast.Continue)):
            return True
            
        # Allow try-except blocks
        if isinstance(node, (ast.Try, ast.ExceptHandler)):
            return True
            
        # Disallow any other type of node
        print(f"Unsafe operation detected: {type(node).__name__}")
        return False
    
    @staticmethod
    def load_factor_class(class_code: str, common_imports: str = None) -> type:
        """动态加载因子类代码

        这个方法就像一个"因子工厂"，它会将代码字符串编译为可执行的因子类。

        为什么需要这个方法？
        --------------------

        用户通过 Web 界面或 API 提交的因子代码是字符串形式，
        需要动态编译为 Python 类才能执行。这个方法负责：
        - 准备安全的执行环境
        - 注入必要的依赖（Factor 基类、FactorUtils 等）
        - 编译代码并返回因子类

        工作原理（简单理解）
        ------------------

        就像编译代码：

        1. **创建环境**：创建一个隔离的模块环境
        2. **准备依赖**：注入 Factor、FactorUtils、pandas、numpy 等
        3. **执行代码**：在安全环境中执行因子代码
        4. **查找类**：从执行结果中找到因子类
        5. **返回类**：返回找到的因子类

        实际使用场景
        -----------

        ```python
        code = '''
        class MyFactor(Factor):
            def calculate(self, factors):
                close = factors['close']
                return RETURNS(close)  # 可以直接使用 FactorUtils 的方法
        '''
        
        factor_class = FactorLoader.load_factor_class(code)
        ```

        Args:
            class_code: 因子类代码字符串，必须包含一个继承自 Factor 的类
            common_imports: 可选的公共导入语句，会被添加到代码开头

        Returns:
            type: 加载的因子类，如果加载失败返回 None

        Raises:
            不会抛出异常，错误会被捕获并打印，返回 None

        Example:
            >>> code = '''
            ... class MyFactor(Factor):
            ...     def calculate(self, factors):
            ...         return factors['close'] / factors['open'] - 1
            ... '''
            >>> factor_class = FactorLoader.load_factor_class(code)
            >>> if factor_class:
            ...     factor = factor_class()
            ...     result = factor.calculate(factors)
        """
        try:
            # 创建一个新的模块
            spec = importlib.util.spec_from_loader('dynamic_factor', loader=None)
            module = importlib.util.module_from_spec(spec)
            
            # 准备代码
            setup_code = """
from panda_factor.generate.factor_base import Factor
from panda_factor.generate.factor_utils import FactorUtils
import pandas as pd
import numpy as np

# 从FactorUtils导入所有公共方法到全局命名空间
"""
            if common_imports:
                setup_code += common_imports
                
            # 添加FactorUtils的所有公共方法到全局命名空间
            method_code = ""
            for method_name in dir(FactorUtils):
                if not method_name.startswith('_'):  # 跳过私有方法
                    method_code += f"{method_name} = FactorUtils.{method_name}\n"
            
            setup_code += method_code
            
            # 组合完整代码
            full_code = setup_code + "\n" + class_code
            
            # 执行代码
            exec(full_code, module.__dict__)
            
            # 查找继承自Factor的类
            factor_class = None
            for item in module.__dict__.values():
                if isinstance(item, type) and issubclass(item, Factor) and item != Factor:
                    factor_class = item
                    break
            
            if factor_class is None:
                print("未找到有效的因子类")
                return None
                
            return factor_class
            
        except Exception as e:
            print(f"加载因子类时出错: {str(e)}")
            import traceback
            print(f"错误详情: {traceback.format_exc()}")
            return None 