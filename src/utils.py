#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用工具函数模块
包含数据迁移过程中的常用工具函数
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import pymysql
from pymysql import Error


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """
    配置日志记录器

    Args:
        name: 日志记录器名称
        log_file: 日志文件路径
        level: 日志级别

    Returns:
        配置好的日志记录器
    """
    # 创建日志目录（如果不存在）
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 防止重复添加处理器
    if logger.handlers:
        return logger

    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # 格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def load_checkpoint(checkpoint_file: str) -> Dict[str, Any]:
    """
    加载检查点文件

    Args:
        checkpoint_file: 检查点文件路径

    Returns:
        检查点数据（字典），如果文件不存在则返回空字典
    """
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"警告：加载检查点文件失败: {e}")
            return {}
    return {}


def save_checkpoint(checkpoint_file: str, checkpoint_data: Dict[str, Any]) -> None:
    """
    保存检查点数据

    Args:
        checkpoint_file: 检查点文件路径
        checkpoint_data: 检查点数据（字典）
    """
    try:
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"警告：保存检查点文件失败: {e}")


def parse_mysql_connection_string(connection_string: str) -> Dict[str, str]:
    """
    解析 MySQL 连接字符串

    Args:
        connection_string: MySQL 连接字符串，格式如：mysql://user:password@host:port/database

    Returns:
        包含连接参数的字典
    """
    # 匹配模式：mysql://user:password@host:port/database
    pattern = r'mysql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
    match = re.match(pattern, connection_string)

    if match:
        return {
            'user': match.group(1),
            'password': match.group(2),
            'host': match.group(3),
            'port': int(match.group(4)),
            'database': match.group(5)
        }
    else:
        raise ValueError(f"无效的 MySQL 连接字符串: {connection_string}")


def get_mysql_config_from_env() -> Dict[str, str]:
    """
    从环境变量加载 MySQL 配置

    Returns:
        包含 MySQL 配置的字典
    """
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '3306')),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_DATABASE', ''),
        'charset': os.getenv('DB_CHARSET', 'utf8mb4')
    }


def connect_to_mysql(config: Dict[str, str], logger: Optional[logging.Logger] = None) -> pymysql.Connection:
    """
    连接到 MySQL 数据库

    Args:
        config: MySQL 配置字典
        logger: 日志记录器（可选）

    Returns:
        MySQL 连接对象

    Raises:
        pymysql.Error: 连接失败时抛出异常
    """
    try:
        connection = pymysql.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config.get('database'),
            charset=config.get('charset', 'utf8mb4'),
            autocommit=False,
            connect_timeout=60,
            read_timeout=360,
            write_timeout=360,
            max_allowed_packet=1073741824  # 1GB
        )

        if logger:
            logger.info(f"成功连接到 MySQL: {config['host']}:{config['port']}/{config.get('database', '')}")

        return connection

    except Error as e:
        if logger:
            logger.error(f"连接 MySQL 失败: {e}")
        raise


def quote_identifier(identifier: str) -> str:
    """
    如果标识符是 MySQL 保留关键字，则用反引号包裹

    Args:
        identifier: 标识符名称

    Returns:
        处理后的标识符
    """
    # MySQL 保留关键字列表（简化版）
    reserved_keywords = {
        'access', 'add', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
        'between', 'bigint', 'binary', 'blob', 'both', 'by', 'cascade',
        'case', 'change', 'char', 'character', 'check', 'collate', 'column',
        'condition', 'constraint', 'continue', 'convert', 'create', 'cross',
        'cube', 'cume_dist', 'current_date', 'current_time', 'current_timestamp',
        'current_user', 'cursor', 'database', 'databases', 'day_hour',
        'day_microsecond', 'day_minute', 'day_second', 'dec', 'decimal',
        'declare', 'default', 'delayed', 'delete', 'dense_rank', 'desc',
        'describe', 'deterministic', 'distinct', 'distinctrow', 'div', 'double',
        'drop', 'dual', 'each', 'else', 'elseif', 'empty', 'enclosed', 'escaped',
        'except', 'exists', 'exit', 'explain', 'false', 'fetch', 'first_value',
        'float', 'float4', 'float8', 'for', 'force', 'foreign', 'from', 'fulltext',
        'function', 'generated', 'get', 'grant', 'group', 'grouping', 'groups',
        'having', 'high_priority', 'hour_microsecond', 'hour_minute', 'hour_second',
        'if', 'ignore', 'in', 'index', 'infile', 'inner', 'inout', 'insensitive',
        'insert', 'int', 'int1', 'int2', 'int3', 'int4', 'int8', 'integer',
        'interval', 'into', 'io_after_gtids', 'io_before_gtids', 'is', 'iterate',
        'join', 'json_table', 'key', 'keys', 'kill', 'last_value', 'lateral',
        'lead', 'leading', 'leave', 'left', 'like', 'limit', 'linear', 'lines',
        'load', 'localtime', 'localtimestamp', 'lock', 'long', 'longblob',
        'longtext', 'loop', 'low_priority', 'master_bind', 'master_ssl_verify_server_cert',
        'match', 'maxvalue', 'mediumblob', 'mediumint', 'mediumtext', 'middleint',
        'minute_microsecond', 'minute_second', 'mod', 'modifies', 'natural',
        'not', 'no_write_to_binlog', 'nth_value', 'ntile', 'null', 'numeric',
        'of', 'on', 'optimize', 'optimizer_costs', 'option', 'optionally', 'or',
        'order', 'out', 'outer', 'outfile', 'over', 'partition', 'percent_rank',
        'precision', 'primary', 'procedure', 'purge', 'range', 'rank', 'read',
        'reads', 'read_write', 'real', 'recursive', 'references', 'regexp',
        'release', 'rename', 'repeat', 'replace', 'require', 'resignal', 'restrict',
        'return', 'revoke', 'right', 'rlike', 'row', 'rows', 'row_number',
        'schema', 'schemas', 'second_microsecond', 'select', 'sensitive', 'separator',
        'set', 'show', 'signal', 'smallint', 'spatial', 'specific', 'sql',
        'sqlexception', 'sqlstate', 'sqlwarning', 'sql_big_result', 'sql_calc_found_rows',
        'sql_small_result', 'ssl', 'starting', 'stored', 'straight_join', 'system',
        'table', 'terminated', 'then', 'tinyblob', 'tinyint', 'tinytext', 'to',
        'trailing', 'trigger', 'true', 'undo', 'union', 'unique', 'unlock', 'unsigned',
        'update', 'usage', 'use', 'using', 'utc_date', 'utc_time', 'utc_timestamp',
        'values', 'varbinary', 'varchar', 'varcharacter', 'varying', 'virtual',
        'when', 'where', 'while', 'window', 'with', 'write', 'xor',
        'year_month', 'zerofill'
    }

    if identifier.lower() in reserved_keywords:
        return f'`{identifier}`'
    return identifier


def convert_sql_server_type_to_mysql(sql_type: str) -> str:
    """
    将 SQL Server 数据类型转换为 MySQL 数据类型

    Args:
        sql_type: SQL Server 数据类型字符串

    Returns:
        MySQL 数据类型字符串
    """
    type_mapping = {
        'numeric': 'decimal',
        'money': 'decimal(19,4)',
        'smallmoney': 'decimal(10,4)',
        'bit': 'int',
        'datetime': 'datetime',
        'smalldatetime': 'datetime',
        'nvarchar': 'varchar',
        'nchar': 'char',
        'ntext': 'text',
        'uniqueidentifier': 'char(36)',
    }

    sql_type_lower = sql_type.lower()

    # 处理带长度的类型
    for sql_key, mysql_val in type_mapping.items():
        if sql_type_lower.startswith(sql_key):
            # 如果是 money 类型，直接替换
            if sql_key == 'money' or sql_key == 'smallmoney':
                return mysql_val
            # 如果是带长度的类型，保留长度信息
            if '(' in sql_type_lower and ')' in sql_type_lower:
                length_part = sql_type[sql_type.find('('):]
                return mysql_val + length_part
            return mysql_val

    # 处理 varchar 长度大于 255 的情况
    if sql_type_lower.startswith('varchar'):
        match = re.search(r'varchar\((\d+)\)', sql_type_lower)
        if match and int(match.group(1)) > 255:
            return 'text'

    # 默认返回原类型
    return sql_type


def remove_brackets(sql: str) -> str:
    """
    移除 SQL Server 的方括号标识符

    Args:
        sql: SQL 字符串

    Returns:
        处理后的 SQL 字符串
    """
    return re.sub(r'\[([^\]]+)\]', r'\1', sql)


def parse_table_name(sql: str) -> str:
    """
    从 CREATE TABLE 或 INSERT INTO 语句中提取表名

    Args:
        sql: SQL 语句

    Returns:
        表名
    """
    # 移除方括号
    sql = remove_brackets(sql)

    # 匹配 CREATE TABLE 或 INSERT INTO
    pattern = r'(?:CREATE TABLE|INSERT INTO)\s+[\w.]*?\.?(\w+)'
    match = re.search(pattern, sql, re.IGNORECASE)

    if match:
        return match.group(1)
    else:
        raise ValueError(f"无法从 SQL 语句中提取表名: {sql}")


def format_bytes(bytes_value: int) -> str:
    """
    格式化字节数为人类可读的格式

    Args:
        bytes_value: 字节数

    Returns:
        格式化后的字符串（如：1.23 MB）
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def format_time(seconds: float) -> str:
    """
    格式化时间为人类可读的格式

    Args:
        seconds: 秒数

    Returns:
        格式化后的字符串（如：1h 23m 45s）
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def retry_on_failure(func, max_retries: int = 3, delay: float = 5.0, logger: Optional[logging.Logger] = None):
    """
    重试装饰器

    Args:
        func: 要重试的函数
        max_retries: 最大重试次数
        delay: 重试延迟（秒）
        logger: 日志记录器（可选）

    Returns:
        函数执行结果

    Raises:
        Exception: 重试次数用尽后抛出异常
    """
    def wrapper(*args, **kwargs):
        last_exception = None

        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    if logger:
                        logger.warning(f"函数 {func.__name__} 执行失败，{delay} 秒后重试 (尝试 {attempt + 1}/{max_retries}): {e}")
                    import time
                    time.sleep(delay)
                else:
                    if logger:
                        logger.error(f"函数 {func.__name__} 重试次数用尽，放弃重试: {e}")

        raise last_exception

    return wrapper


def print_summary(total: int, success: int, error: int, start_time: datetime) -> None:
    """
    打印执行摘要

    Args:
        total: 总数
        success: 成功数
        error: 错误数
        start_time: 开始时间
    """
    elapsed_time = (datetime.now() - start_time).total_seconds()
    success_rate = (success / total * 100) if total > 0 else 0

    print("\n" + "=" * 80)
    print("执行摘要")
    print("=" * 80)
    print(f"总计:     {total}")
    print(f"成功:     {success}")
    print(f"失败:     {error}")
    print(f"成功率:   {success_rate:.2f}%")
    print(f"耗时:     {format_time(elapsed_time)}")
    print("=" * 80)
