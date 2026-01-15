# 数据迁移项目

这是一个使用Python和uv管理的数据分析和迁移项目，支持多种数据源格式到MySQL的迁移。

## 🎯 项目特性

- **多种数据源支持**: SQL Server SQL 文件、XLSX、CSV、JSON
- **两种迁移方案**:
  - 传统方案（SQL Server → 本地MySQL → 线上MySQL）
  - DuckDB 方案（SQL Server/XLSX/CSV/JSON → DuckDB → 线上MySQL）
- **高性能迁移**: DuckDB 方案预计减少 70-75% 的迁移时间
- **断点续传**: 支持中断后继续迁移
- **批量处理**: 支持批量插入，提升性能
- **完善的错误处理**: 自动重试机制，详细的错误日志

## 📁 项目结构

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
# 安装所有依赖
uv sync
```

---

## 🔄 传统方案（保留）

传统方案仍然可用，适合需要逐步迁移或调试的场景。

### 步骤 1: SQL Server 到 MySQL 语法转换

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

---

## 🔧 故障排查

### DuckDB 方案常见问题

#### 1. 连接 MySQL 失败

**错误信息**: `Access denied for user 'xxx'@'xxx'`

**解决方案**:
- 检查 `.env` 文件中的数据库配置是否正确
- 确认 MySQL 用户有足够的权限
- 检查 MySQL 服务器是否允许外部连接

#### 2. 内存不足

**错误信息**: `Out of memory`

**解决方案**:
- 减小 `BATCH_SIZE` 参数（例如从 10000 降至 5000）
- 减小 `DUCKDB_MEMORY_LIMIT` 参数
- 分批处理大文件

#### 3. 数据类型转换错误

**错误信息**: `Data type conversion failed`

**解决方案**:
- 使用 `--dry-run` 模式查看数据结构
- 检查源数据是否有异常值
- 考虑在导入前清洗源数据

#### 4. 网络中断

**错误信息**: `Lost connection to MySQL server`

**解决方案**:
- 使用 `--resume` 参数从检查点继续
- 增加 `--max-retries` 参数
- 检查网络连接稳定性

### 传统方案常见问题

#### 1. SQL 语法转换失败

**错误信息**: `Syntax error in converted SQL`

**解决方案**:
- 检查原始 SQL 文件是否符合 SQL Server 语法
- 查看转换后的 SQL 文件，手动修复问题
- 参考 `src/sql_server_to_mysql.py` 中的转换规则

#### 2. SQL 执行失败

**错误信息**: `Execution failed for statement X`

**解决方案**:
- 查看 `sql_execution_errors.jsonl` 文件了解详细错误
- 使用 `--dry-run` 模式预览 SQL 语句
- 检查 MySQL 数据库是否有足够的权限

---

## 📈 性能优化建议

### DuckDB 方案优化

1. **批量大小**: 根据内存大小调整 `--batch-size`
   - 8GB 内存：建议 50000-100000
   - 4GB 内存：建议 10000-50000
   - 2GB 内存：建议 5000-10000

2. **并发线程**: 根据 CPU 核心数设置 `DUCKDB_THREADS`
   - 建议设置为 CPU 核心数

3. **内存限制**: 设置合适的 `DUCKDB_MEMORY_LIMIT`
   - 建议设置为系统可用内存的 70-80%

4. **网络优化**:
   - 使用稳定的网络连接
   - 增加 `--max-retries` 和 `RETRY_DELAY`

### 传统方案优化

1. **批量执行**: 在 `mysql_sql_executor.py` 中调整批量大小

2. **禁用约束**: 使用 `--disable-constraints` 参数提升导入速度

3. **使用 LOAD DATA INFILE**: 对于大量数据，使用 LOAD DATA INFILE 比 INSERT 更快

---

## 📊 监控和日志

### DuckDB 方案日志

日志文件位置: `logs/duckdb_migration_YYYYMMDD_HHMMSS.log`

日志包含:
- 详细的迁移步骤
- 每个表的处理进度
- 错误信息和堆栈跟踪
- 性能统计

### 检查点文件

检查点文件位置: `logs/migration_checkpoint.json`

检查点记录:
- 每个表的导入偏移量
- 完成状态
- 可用于断点续传

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 开发环境设置

```bash
# 克隆项目
git clone <repository-url>
cd data-migration

# 安装开发依赖
uv sync --dev

# 运行测试（如果存在）
uv run pytest
```

---

## 📝 许可证

本项目采用 MIT 许可证。

---

## 📞 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 Issue
- 发送邮件

---

## 🎉 致谢

感谢以下开源项目：

- [DuckDB](https://duckdb.org/) - 高性能分析数据库
- [PyMySQL](https://pymysql.readthedocs.io/) - MySQL Python 客户端
- [pandas](https://pandas.pydata.org/) - 数据分析库
- [tqdm](https://tqdm.github.io/) - 进度条库
