# 数据迁移项目

这是一个使用Python和uv管理的数据分析和迁移项目。

## 项目结构

- `src/`: 源代码目录
- `main.py`: 主程序入口
- `pyproject.toml`: 项目配置文件
- `sql_server_to_mysql.py`: SQL Server到MySQL转换工具

## SQL Server到MySQL转换工具

### 功能特性

- **IDENTITY 转换**: 将 `IDENTITY(1,1)` 转换为 `AUTO_INCREMENT`
- **标识符处理**: 移除SQL Server的方括号标识符 `[]`
- **COLLATE 移除**: 移除SQL Server特有的 `COLLATE` 子句
- **数据类型转换**: 将 `numeric` 转换为 `decimal`
- **主键约束**: 自动添加 `PRIMARY KEY` 约束

### 使用方法

```bash
# 基本用法
uv run src/sql_server_to_mysql.py <SQL文件路径>

# 示例
uv run src/sql_server_to_mysql.py source-data/DRecipe处方.sql
```

转换后会在同目录下生成 `*_mysql.sql` 文件。

### 支持的转换规则

| SQL Server 语法 | MySQL 语法 |
|----------------|------------|
| `[column_name]` | `column_name` |
| `IDENTITY(1,1)` | `AUTO_INCREMENT` |
| `numeric(p,s)` | `decimal(p,s)` |
| `COLLATE Chinese_PRC_CI_AS` | (移除) |
| `CREATE TABLE [dbo].[table_name]` | `CREATE TABLE table_name` |

## 安装

```bash
uv sync
```

## MySQL SQL执行器

### 功能特性

- **SQL语句分割**: 智能按分号分割SQL语句，正确处理字符串中的分号
- **错误处理**: 单条语句执行失败不影响其他语句执行
- **干运行模式**: 支持只解析不执行的模式，便于预览
- **连接配置**: 支持自定义MySQL连接参数
- **环境变量支持**: 支持从.env文件读取数据库配置

### 使用方法

```bash
# 基本用法
uv run src/mysql_sql_executor.py <SQL文件路径> --database <数据库名>

# 使用.env文件配置
# 复制示例配置文件并修改为您的实际配置：
# cp .env.example .env
# 编辑.env文件，填入您的实际数据库配置

# .env文件示例内容：
# DB_HOST=localhost
# DB_PORT=3306
# DB_USER=root
# DB_PASSWORD=yourpassword
# DB_DATABASE=your_database
# DB_CHARSET=utf8mb4

# 使用.env文件中的配置执行SQL
uv run src/mysql_sql_executor.py target-data/DRecipe处方_mysql.sql

# 完整参数示例
uv run src/mysql_sql_executor.py target-data/DRecipe处方_mysql.sql \
  --host localhost \
  --port 3306 \
  --user root \
  --password yourpassword \
  --database your_database \
  --charset utf8mb4

# 干运行模式（只解析不执行）
uv run src/mysql_sql_executor.py target-data/DRecipe处方_mysql.sql \
  --database your_database \
  --dry-run
```

### 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| sql_file | 要执行的SQL文件路径 | 必需 |
| --host | MySQL服务器地址 | 从.env文件读取 |
| --port | MySQL服务器端口 | 从.env文件读取 |
| --user | MySQL用户名 | 从.env文件读取 |
| --password | MySQL密码 | 从.env文件读取 |
| --database | 要连接的数据库名 | 从.env文件读取 |
| --charset | 字符集 | 从.env文件读取 |
| --dry-run | 只解析SQL语句，不实际执行 | False |
