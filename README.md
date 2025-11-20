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
python3 src/sql_server_to_mysql.py <SQL文件路径>

# 示例
python3 src/sql_server_to_mysql.py source-data/DRecipe处方.sql
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

## 使用方法

```bash
uv run python main.py
```