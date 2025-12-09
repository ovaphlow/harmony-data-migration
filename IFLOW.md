# 数据迁移项目 iFlow 上下文

## 项目概述

这是一个使用 Python 和 uv 包管理器构建的数据迁移项目，专门用于将 SQL Server 数据库迁移到 MySQL。项目提供了完整的转换和执行流程，支持 SQL Server 语法到 MySQL 兼容语法的自动转换，并能直接在 MySQL 数据库中执行转换后的 SQL 语句。

### 主要技术栈

- **Python**: >= 3.13
- **包管理器**: uv
- **数据库连接**: PyMySQL
- **配置管理**: python-dotenv
- **项目结构**: 模块化设计，功能分离

### 核心功能

1. **SQL Server 到 MySQL 语法转换**
   - IDENTITY 转换为 AUTO_INCREMENT
   - 移除方括号标识符
   - 移除 COLLATE 子句
   - 数据类型转换 (numeric → decimal, money → decimal)
   - 自动添加 PRIMARY KEY 约束
   - 处理复杂的 ALTER TABLE 语句
   - 支持 IDENTITY_INSERT 语句转换

2. **MySQL SQL 执行器**
   - 智能按分号分割 SQL 语句
   - 错误处理和日志记录（支持 JSON 格式和彩色控制台输出）
   - 干运行模式支持
   - 环境变量配置支持
   - 详细的执行统计和进度报告
   - 错误不中断执行机制

## 项目结构

```
data-migration/
├── main.py                    # 主程序入口（简单示例）
├── pyproject.toml            # 项目配置和依赖
├── README.md                 # 项目详细文档
├── .env.example              # 数据库配置示例
├── .python-version           # Python 版本指定 (3.13)
├── uv.lock                   # 依赖锁定文件
├── src/                      # 源代码目录
│   ├── sql_server_to_mysql.py    # SQL Server 到 MySQL 转换工具
│   └── mysql_sql_executor.py     # MySQL SQL 执行器
├── source-data/              # 源数据目录（SQL Server 文件）
└── target-data/              # 目标数据目录（MySQL 文件）
```

## 构建和运行

### 环境准备

```bash
# 安装依赖
uv sync

# 复制环境配置文件
cp .env.example .env
# 编辑 .env 文件，填入实际的数据库配置
```

### 主要运行命令

1. **仅转换 SQL 语法**
```bash
# 使用 uv 运行（推荐）
uv run src/sql_server_to_mysql.py source-data/DRecipe处方.sql

# 直接使用 Python
python3 src/sql_server_to_mysql.py source-data/DRecipe处方.sql
```

2. **仅执行已转换的 MySQL 文件**
```bash
# 使用环境配置
uv run src/mysql_sql_executor.py target-data/DRecipe处方_mysql.sql --database your_database

# 使用命令行参数
uv run src/mysql_sql_executor.py target-data/DRecipe处方_mysql.sql \
  --database your_database \
  --user root \
  --password yourpassword
```

3. **干运行模式（预览）**
```bash
uv run src/mysql_sql_executor.py target-data/DRecipe处方_mysql.sql \
  --database your_database \
  --dry-run
```

### 测试和验证

项目目前没有正式的测试套件，但可以通过以下方式进行验证：

```bash
# 测试转换功能（不执行）
uv run src/sql_server_to_mysql.py source-data/test_file.sql

# 测试执行功能（干运行）
uv run src/mysql_sql_executor.py target-data/test_file_mysql.sql \
  --database your_database \
  --dry-run
```

## 开发约定

### 代码风格

- 使用 Python 3.13+ 语法特性
- 遵循 PEP 8 代码风格规范
- 使用 UTF-8 编码
- 详细的文档字符串和注释

### 错误处理

- 所有数据库操作都有完善的错误处理
- 使用 Python 标准库的 logging 模块记录日志
- 支持彩色控制台输出和 JSON 格式文件日志
- 错误不会中断整个迁移流程
- 提供详细的错误信息和统计

### 配置管理

- 优先使用环境变量（.env 文件）
- 支持命令行参数覆盖环境配置
- 敏感信息（如密码）不硬编码在代码中

### 文件命名

- 源文件使用描述性名称
- 转换后的文件添加 `_mysql` 后缀
- 配置文件使用 `.env` 格式

## 核心模块说明

### sql_server_to_mysql.py

负责 SQL Server 语法到 MySQL 的转换，主要功能：

- 正则表达式预编译提高性能
- 处理多行和单行注释
- 转换标识符、数据类型和约束
- 处理复杂的 ALTER TABLE 和主键约束
- 支持 IDENTITY_INSERT 语句
- 生成转换报告和统计信息

**支持的转换规则**:
| SQL Server 语法 | MySQL 语法 |
|----------------|------------|
| `[column_name]` | `column_name` |
| `IDENTITY(1,1)` | `AUTO_INCREMENT` |
| `numeric(p,s)` | `decimal(p,s)` |
| `money` | `decimal(19,4)` |
| `COLLATE Chinese_PRC_CI_AS` | (移除) |
| `CREATE TABLE [dbo].[table_name]` | `CREATE TABLE table_name` |

### mysql_sql_executor.py

负责在 MySQL 数据库中执行 SQL 语句，主要功能：

- 智能分割 SQL 语句（处理字符串中的分号）
- JSON 格式的结构化日志记录
- 彩色控制台输出格式化器
- 连接池管理和错误重试
- 执行统计和进度报告
- 干运行模式支持

**日志功能**:
- 支持 JSON 格式文件日志
- 彩色控制台输出，不同级别使用不同颜色
- 详细的执行上下文信息记录
- 错误日志和成功统计

## 数据库配置

### 环境变量配置

```bash
# 数据库服务器地址
DB_HOST=localhost

# 数据库服务器端口
DB_PORT=3306

# 数据库用户名
DB_USER=root

# 数据库密码
DB_PASSWORD=yourpassword

# 要连接的数据库名
DB_DATABASE=your_database

# 数据库字符集
DB_CHARSET=utf8mb4
```

## 常见使用场景

1. **一次性的完整迁移**
   - 将 SQL Server 导出的 SQL 文件直接迁移到 MySQL
   - 适用于数据库迁移项目

2. **批量文件处理**
   - 处理多个 SQL Server 文件
   - 可以编写脚本批量调用转换工具

3. **开发和测试环境**
   - 快速将生产环境的 SQL Server 结构迁移到测试 MySQL 环境
   - 支持干运行模式进行预览

4. **错误分析**
   - 详细的日志记录帮助分析迁移过程中的问题
   - JSON 格式日志便于自动化分析

## 注意事项

- 确保目标 MySQL 数据库已创建
- 转换前备份重要数据
- 大型文件可能需要调整数据库连接参数
- 某些 SQL Server 特有功能可能需要手动调整
- 转换工具会自动在 target-data 目录生成转换后的文件
- 执行器支持错误不中断模式，会继续执行后续语句

## 项目特点

- **高性能**: 使用预编译正则表达式和优化的字符串处理
- **健壮性**: 完善的错误处理和日志记录
- **易用性**: 简单的命令行界面和详细的使用说明
- **可维护性**: 模块化设计和清晰的代码结构
- **可观测性**: 详细的执行统计和日志记录