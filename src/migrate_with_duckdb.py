#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用 DuckDB 进行数据迁移的主程序
支持 SQL Server SQL 文件、XLSX、CSV、JSON 等多种数据源格式
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any

from dotenv import load_dotenv

from data_reader import DataReader
from duckdb_processor import DuckDBProcessor
from utils import (
    setup_logger,
    load_checkpoint,
    save_checkpoint,
    get_mysql_config_from_env,
    format_time,
    print_summary
)

# 加载环境变量
load_dotenv()


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='使用 DuckDB 进行数据迁移（SQL Server/XLSX/CSV/JSON → MySQL）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 迁移 SQL Server SQL 文件
  python src/migrate_with_duckdb.py source-data/elderDiagnose.sql --database your_database

  # 迁移 XLSX 文件
  python src/migrate_with_duckdb.py data.xlsx --database your_database --type xlsx

  # 迁移 CSV 文件
  python src/migrate_with_duckdb.py data.csv --database your_database --type csv --table-name table_name

  # 批量迁移多个 SQL 文件
  python src/migrate_with_duckdb.py source-data/*.sql --database your_database

  # 指定批量大小和重试次数
  python src/migrate_with_duckdb.py data.sql --database your_database --batch-size 50000 --max-retries 5

  # 干运行模式（仅解析，不执行）
  python src/migrate_with_duckdb.py data.sql --database your_database --dry-run

  # 从检查点继续
  python src/migrate_with_duckdb.py source-data/*.sql --database your_database --resume
        """
    )

    # 必需参数
    parser.add_argument('input_files', nargs='+', help='输入文件路径（支持通配符）')

    # 数据源类型
    parser.add_argument('--type', '-t',
                       choices=['sql', 'xlsx', 'csv', 'json'],
                       help='数据源类型（默认根据文件扩展名自动识别）')

    # MySQL 配置
    parser.add_argument('--database', '-d',
                       help='目标数据库名称（默认从环境变量 DB_DATABASE 读取）')
    parser.add_argument('--host',
                       help='MySQL 主机地址（默认从环境变量 DB_HOST 读取）')
    parser.add_argument('--port', type=int,
                       help='MySQL 端口（默认从环境变量 DB_PORT 读取）')
    parser.add_argument('--user',
                       help='MySQL 用户名（默认从环境变量 DB_USER 读取）')
    parser.add_argument('--password',
                       help='MySQL 密码（默认从环境变量 DB_PASSWORD 读取）')

    # DuckDB 配置
    parser.add_argument('--batch-size', '-b', type=int, default=10000,
                       help='批量插入大小（默认：10000）')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='失败重试次数（默认：3）')

    # 表名（用于 CSV/JSON）
    parser.add_argument('--table-name',
                       help='表名（用于 CSV/JSON 文件，默认使用文件名）')

    # 执行选项
    parser.add_argument('--dry-run', action='store_true',
                       help='干运行模式：仅解析数据，不实际导入到 MySQL')
    parser.add_argument('--resume', '-r', action='store_true',
                       help='从检查点继续迁移')
    parser.add_argument('--create-table', action='store_true', default=True,
                       help='自动创建表（默认：True）')
    parser.add_argument('--no-create-table', action='store_false', dest='create_table',
                       help='不自动创建表')

    # 日志选项
    parser.add_argument('--log-level',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       default='INFO',
                       help='日志级别（默认：INFO）')
    parser.add_argument('--log-dir', default='logs',
                       help='日志目录（默认：logs）')

    return parser.parse_args()


def detect_file_type(file_path: str) -> str:
    """
    根据文件扩展名和内容检测文件类型

    Args:
        file_path: 文件路径

    Returns:
        文件类型（sql_server_sql, mysql_sql, xlsx, csv, json）
    """
    ext = os.path.splitext(file_path)[1].lower()

    type_mapping = {
        '.sql': 'sql',
        '.xlsx': 'xlsx',
        '.xls': 'xlsx',
        '.csv': 'csv',
        '.json': 'json'
    }

    file_type = type_mapping.get(ext, 'sql')

    # 如果是 SQL 文件，进一步检测是 SQL Server 还是 MySQL 格式
    if file_type == 'sql':
        sql_format = detect_sql_file_type(file_path)
        if sql_format == 'mysql':
            return 'mysql_sql'
        else:
            return 'sql_server_sql'

    return file_type


def detect_sql_file_type(file_path: str) -> str:
    """
    检测 SQL 文件的格式类型

    Args:
        file_path: SQL 文件路径

    Returns:
        'sql_server' 或 'mysql'
    """
    # 1. 如果文件名包含 '_mysql'，则为 MySQL 格式
    if '_mysql' in file_path:
        return 'mysql'

    # 2. 检查文件内容是否包含 GO 语句（SQL Server 特有）
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # 读取前 1000 行检查
            content = '\n'.join([f.readline() for _ in range(1000)])
            if re.search(r'\bGO\b', content):
                return 'sql_server'
    except Exception as e:
        print(f"警告：无法读取文件 {file_path} 检测格式: {e}")

    # 3. 默认返回 'mysql'
    return 'mysql'


def validate_input_files(file_paths: List[str]) -> List[str]:
    """
    验证输入文件是否存在

    Args:
        file_paths: 文件路径列表

    Returns:
        有效的文件路径列表
    """
    valid_files = []

    for file_path in file_paths:
        if os.path.exists(file_path) and os.path.isfile(file_path):
            valid_files.append(file_path)
        else:
            print(f"警告：文件不存在或不是文件: {file_path}")

    return valid_files


def read_data_source(
    file_path: str,
    file_type: str,
    reader: DataReader,
    table_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    读取数据源

    Args:
        file_path: 文件路径
        file_type: 文件类型
        reader: 数据读取器
        table_name: 表名（可选）

    Returns:
        数据字典
    """
    print(f"\n{'='*80}")
    print(f"读取文件: {file_path}")
    print(f"{'='*80}")

    if file_type == 'sql_server_sql':
        data = reader.read_sql_server(file_path)
    elif file_type == 'mysql_sql':
        data = reader.read_mysql_sql(file_path)
    elif file_type == 'xlsx':
        data = reader.read_excel(file_path)
    elif file_type == 'csv':
        data = reader.read_csv(file_path, table_name)
    elif file_type == 'json':
        data = reader.read_json(file_path, table_name)
    else:
        raise ValueError(f"不支持的文件类型: {file_type}")

    # 统计信息
    total_rows = sum(len(df) for df in data.values())
    print(f"✓ 成功读取 {len(data)} 个表，共 {total_rows} 行数据\n")

    return data


def main():
    """主函数"""
    args = parse_arguments()

    # 设置日志
    os.makedirs(args.log_dir, exist_ok=True)
    log_file = os.path.join(args.log_dir, f'duckdb_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logger = setup_logger('duckdb_migration', log_file, level=getattr(logging, args.log_level))

    logger.info("="*80)
    logger.info("DuckDB 数据迁移工具启动")
    logger.info("="*80)

    start_time = datetime.now()

    try:
        # 验证输入文件
        input_files = validate_input_files(args.input_files)

        if not input_files:
            logger.error("没有有效的输入文件")
            return 1

        logger.info(f"找到 {len(input_files)} 个有效文件")

        # 获取 MySQL 配置
        mysql_config = get_mysql_config_from_env()

        # 命令行参数覆盖环境变量
        if args.host:
            mysql_config['host'] = args.host
        if args.port:
            mysql_config['port'] = args.port
        if args.user:
            mysql_config['user'] = args.user
        if args.password:
            mysql_config['password'] = args.password
        if args.database:
            mysql_config['database'] = args.database

        # 验证数据库名
        if not mysql_config['database']:
            logger.error("未指定数据库名，请使用 --database 参数或设置 DB_DATABASE 环境变量")
            return 1

        logger.info(f"目标数据库: {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")

        # 初始化数据读取器
        reader = DataReader(logger)

        # 初始化 DuckDB 处理器
        processor = DuckDBProcessor(
            mysql_config=mysql_config,
            logger=logger,
            batch_size=args.batch_size,
            max_retries=args.max_retries
        )

        # 加载检查点
        checkpoint_file = os.path.join(args.log_dir, 'migration_checkpoint.json')
        checkpoint_data = {}
        if args.resume:
            checkpoint_data = load_checkpoint(checkpoint_file)
            logger.info(f"从检查点继续: {checkpoint_data}")

        # 统计信息
        total_files = len(input_files)
        processed_files = 0
        all_results = []

        # 处理每个文件
        for file_path in input_files:
            try:
                # 检测文件类型
                file_type = args.type or detect_file_type(file_path)
                logger.info(f"文件类型: {file_type}")

                # 读取数据
                tables = read_data_source(file_path, file_type, reader, args.table_name)

                if not tables:
                    logger.warning(f"文件 {file_path} 不包含任何数据，跳过")
                    continue

                # 干运行模式：仅显示信息，不执行
                if args.dry_run:
                    logger.info("干运行模式：仅解析数据，不执行导入")
                    for table_name, df in tables.items():
                        print(f"  表名: {table_name}")
                        print(f"  行数: {len(df)}")
                        print(f"  列数: {len(df.columns)}")
                        print(f"  列名: {list(df.columns)}")
                        print()
                    processed_files += 1
                    continue

                # 初始化处理器（第一次）
                if not processor.duckdb_conn:
                    processor.initialize()

                # 导入数据到 DuckDB
                processor.import_data(tables)

                # 导出数据到 MySQL
                for table_name in tables.keys():
                    # 检查是否已处理过该表
                    if checkpoint_data and table_name in checkpoint_data:
                        if checkpoint_data[table_name].get('completed', False):
                            logger.info(f"表 {table_name} 已完成，跳过")
                            continue

                    # 转换数据
                    processor.transform_data(table_name)

                    # 导出到 MySQL
                    result = processor.export_to_mysql(
                        table_name,
                        create_table=args.create_table,
                        resume=args.resume,
                        checkpoint_data=checkpoint_data
                    )

                    all_results.append(result)

                    # 更新检查点
                    if args.resume:
                        checkpoint_data[table_name] = {
                            'offset': result['success_rows'] + result['error_rows'],
                            'completed': True
                        }
                        save_checkpoint(checkpoint_file, checkpoint_data)

                processed_files += 1

            except Exception as e:
                logger.error(f"处理文件 {file_path} 时发生错误: {e}", exc_info=True)
                # 继续处理下一个文件
                continue

        # 关闭处理器
        if not args.dry_run:
            processor.close()

        # 计算总统计
        total_rows = sum(r['total_rows'] for r in all_results)
        success_rows = sum(r['success_rows'] for r in all_results)
        error_rows = sum(r['error_rows'] for r in all_results)

        # 打印摘要
        print("\n" + "="*80)
        print("迁移摘要")
        print("="*80)
        print(f"处理文件: {processed_files}/{total_files}")
        print(f"处理表数: {len(all_results)}")
        print(f"总行数:   {total_rows}")
        print(f"成功行数: {success_rows}")
        print(f"失败行数: {error_rows}")
        success_rate = (success_rows / total_rows * 100) if total_rows > 0 else 0
        print(f"成功率:   {success_rate:.2f}%")
        print(f"总耗时:   {format_time((datetime.now() - start_time).total_seconds())}")
        print("="*80)

        logger.info("数据迁移完成")

        # 如果有错误，返回非零退出码
        if error_rows > 0:
            return 1

        return 0

    except KeyboardInterrupt:
        logger.warning("用户中断迁移")
        return 130
    except Exception as e:
        logger.error(f"迁移失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
