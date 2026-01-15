#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB 数据处理引擎
使用 DuckDB 进行数据处理和批量导出到 MySQL
"""

import logging
from typing import Dict, List, Optional, Any

import duckdb
import pandas as pd
import pymysql
from tqdm import tqdm

from utils import (
    connect_to_mysql,
    quote_identifier,
    format_time,
    retry_on_failure
)


class DuckDBProcessor:
    """DuckDB 数据处理引擎"""

    def __init__(
        self,
        mysql_config: Dict[str, str],
        logger: Optional[logging.Logger] = None,
        batch_size: int = 10000,
        max_retries: int = 3
    ):
        """
        初始化 DuckDB 处理引擎

        Args:
            mysql_config: MySQL 配置字典
            logger: 日志记录器（可选）
            batch_size: 批量插入大小
            max_retries: 失败重试次数
        """
        self.mysql_config = mysql_config
        self.logger = logger or logging.getLogger(__name__)
        self.batch_size = batch_size
        self.max_retries = max_retries

        # 初始化 DuckDB
        self.duckdb_conn = None
        self.mysql_conn = None
        self.mysql_cursor = None

        # 统计信息
        self.stats = {
            'total_tables': 0,
            'total_rows': 0,
            'success_rows': 0,
            'error_rows': 0
        }

    def initialize(self):
        """初始化数据库连接"""
        self.logger.info("初始化 DuckDB 处理引擎...")

        # 初始化 DuckDB
        self.duckdb_conn = duckdb.connect(database=':memory:')
        self.logger.info("DuckDB 连接已建立")

        # 连接 MySQL
        self.mysql_conn = connect_to_mysql(self.mysql_config, self.logger)
        self.mysql_cursor = self.mysql_conn.cursor()
        self.logger.info("MySQL 连接已建立")

    def import_data(self, tables: Dict[str, pd.DataFrame]) -> None:
        """
        导入数据到 DuckDB

        Args:
            tables: 表名 -> DataFrame 的字典
        """
        self.logger.info(f"开始导入 {len(tables)} 个表到 DuckDB...")

        for table_name, df in tables.items():
            self.logger.info(f"导入表 {table_name}: {len(df)} 行")

            # 将 DataFrame 注册到 DuckDB
            self.duckdb_conn.register(table_name, df)

            # 统计信息
            self.stats['total_tables'] += 1
            self.stats['total_rows'] += len(df)

        self.logger.info(f"完成导入，共 {self.stats['total_tables']} 个表，{self.stats['total_rows']} 行")

    def transform_data(self, table_name: str) -> None:
        """
        数据转换和清洗

        Args:
            table_name: 表名
        """
        self.logger.info(f"转换表 {table_name} 数据...")

        # 获取列信息
        columns = self.duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()
        self.logger.debug(f"表 {table_name} 列信息: {columns}")

        # 清理数据（根据需要添加）
        # 例如：处理空值、数据类型转换等

        self.logger.info(f"完成表 {table_name} 数据转换")

    def export_to_mysql(
        self,
        table_name: str,
        create_table: bool = True,
        resume: bool = False,
        checkpoint_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        将数据从 DuckDB 导出到 MySQL

        Args:
            table_name: 表名
            create_table: 是否创建表
            resume: 是否从检查点继续
            checkpoint_data: 检查点数据

        Returns:
            执行统计信息
        """
        self.logger.info(f"开始导出表 {table_name} 到 MySQL...")

        # 获取总行数
        total_count = self.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.logger.info(f"表 {table_name} 共 {total_count} 行")

        # 获取列信息
        columns = self._get_table_columns(table_name)
        self.logger.debug(f"列信息: {columns}")

        # 创建表（如果需要）
        if create_table:
            self._create_mysql_table(table_name, columns)

        # 确定起始位置
        if resume and checkpoint_data and table_name in checkpoint_data:
            offset = checkpoint_data[table_name].get('offset', 0)
            self.logger.info(f"从检查点继续，偏移量: {offset}")
        else:
            offset = 0

        # 批量导出
        success_count = 0
        error_count = 0

        # 使用 tqdm 显示进度
        with tqdm(total=total_count, desc=f"导出 {table_name}", unit="行") as pbar:
            pbar.update(offset)

            while offset < total_count:
                try:
                    # 从 DuckDB 读取一批数据
                    batch = self.duckdb_conn.execute(f"""
                        SELECT * FROM {table_name}
                        LIMIT {self.batch_size} OFFSET {offset}
                    """).fetchall()

                    if not batch:
                        break

                    # 转换为 MySQL 兼容的格式
                    mysql_batch = self._convert_batch_for_mysql(batch, columns)

                    # 构建批量 INSERT 语句
                    placeholders = ', '.join(['%s'] * len(columns))
                    quoted_columns = [quote_identifier(col[0]) for col in columns]
                    sql = f"""
                        INSERT INTO {quote_identifier(table_name)}
                        ({', '.join(quoted_columns)})
                        VALUES ({placeholders})
                    """

                    # 批量插入
                    self.mysql_cursor.executemany(sql, mysql_batch)
                    self.mysql_conn.commit()

                    batch_count = len(batch)
                    success_count += batch_count
                    offset += batch_count

                    # 更新进度
                    pbar.update(batch_count)
                    pbar.set_postfix({
                        '成功': success_count,
                        '错误': error_count
                    })

                    # 更新检查点
                    if resume:
                        self._update_checkpoint(table_name, offset)

                except Exception as e:
                    error_count += min(self.batch_size, total_count - offset)
                    self.logger.error(f"批量导出失败 (offset={offset}): {e}")
                    self.mysql_conn.rollback()

                    # 重试
                    if error_count < self.max_retries * self.batch_size:
                        self.logger.info("5 秒后重试...")
                        import time
                        time.sleep(5)
                    else:
                        self.logger.error(f"重试次数超过限制，跳过表 {table_name}")
                        break

        # 更新统计信息
        self.stats['success_rows'] += success_count
        self.stats['error_rows'] += error_count

        # 返回执行结果
        result = {
            'table_name': table_name,
            'total_rows': total_count,
            'success_rows': success_count,
            'error_rows': error_count,
            'success_rate': (success_count / total_count * 100) if total_count > 0 else 0
        }

        self.logger.info(f"完成导出表 {table_name}: {result}")

        return result

    def _get_table_columns(self, table_name: str) -> List[tuple[str, str]]:
        """
        获取表的列信息

        Args:
            table_name: 表名

        Returns:
            列信息列表：[(列名, 数据类型), ...]
        """
        columns = self.duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()
        return [(col[0], col[1]) for col in columns]

    def _create_mysql_table(self, table_name: str, columns: List[tuple[str, str]]) -> None:
        """
        在 MySQL 中创建表

        Args:
            table_name: 表名
            columns: 列信息列表
        """
        self.logger.info(f"创建 MySQL 表 {table_name}...")

        # 检查表是否存在
        self.mysql_cursor.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        """, (self.mysql_config['database'], table_name))

        exists = self.mysql_cursor.fetchone()[0] > 0

        if exists:
            self.logger.info(f"表 {table_name} 已存在，跳过创建")
            return

        # 构建 CREATE TABLE 语句
        column_defs = []
        for col_name, col_type in columns:
            quoted_name = quote_identifier(col_name)
            mysql_type = self._convert_duckdb_type_to_mysql(col_type)
            column_defs.append(f"{quoted_name} {mysql_type}")

        create_sql = f"""
            CREATE TABLE {quote_identifier(table_name)} (
                {', '.join(column_defs)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        self.logger.debug(f"CREATE TABLE SQL: {create_sql}")

        # 执行创建表
        self.mysql_cursor.execute(create_sql)
        self.mysql_conn.commit()

        self.logger.info(f"成功创建表 {table_name}")

    def _convert_duckdb_type_to_mysql(self, duckdb_type: str) -> str:
        """
        将 DuckDB 数据类型转换为 MySQL 数据类型

        Args:
            duckdb_type: DuckDB 数据类型

        Returns:
            MySQL 数据类型
        """
        type_mapping = {
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'SMALLINT': 'SMALLINT',
            'TINYINT': 'TINYINT',
            'FLOAT': 'FLOAT',
            'DOUBLE': 'DOUBLE',
            'DECIMAL': 'DECIMAL',
            'VARCHAR': 'VARCHAR',
            'TEXT': 'TEXT',
            'BLOB': 'BLOB',
            'BOOLEAN': 'TINYINT(1)',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'DATETIME',
            'UUID': 'CHAR(36)',
            'JSON': 'JSON'
        }

        # 提取基础类型（去掉括号部分）
        base_type = duckdb_type.upper().split('(')[0]

        # 如果是复合类型（如 VARCHAR(255)），保留长度信息
        if '(' in duckdb_type.upper():
            if base_type in type_mapping:
                mysql_type = type_mapping[base_type]
                # 如果原类型有长度，保留长度
                if '(' in duckdb_type:
                    length_part = duckdb_type[duckdb_type.find('('):]
                    return mysql_type + length_part
                return mysql_type

        return type_mapping.get(base_type, 'TEXT')

    def _convert_batch_for_mysql(self, batch: List[tuple], columns: List[tuple[str, str]]) -> List[tuple]:
        """
        将 DuckDB 批次数据转换为 MySQL 兼容格式

        Args:
            batch: DuckDB 批次数据
            columns: 列信息

        Returns:
            MySQL 兼容的批次数据
        """
        mysql_batch = []

        for row in batch:
            mysql_row = []
            for i, value in enumerate(row):
                col_name, col_type = columns[i]

                # 处理 None 值
                if value is None:
                    mysql_row.append(None)
                    continue

                # 处理特殊类型
                duckdb_type = col_type.upper()

                if duckdb_type == 'BOOLEAN':
                    mysql_row.append(1 if value else 0)
                elif duckdb_type in ('DATE', 'TIME', 'TIMESTAMP'):
                    if hasattr(value, 'strftime'):
                        mysql_row.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        mysql_row.append(str(value))
                else:
                    mysql_row.append(value)

            mysql_batch.append(tuple(mysql_row))

        return mysql_batch

    def _update_checkpoint(self, table_name: str, offset: int) -> None:
        """
        更新检查点

        Args:
            table_name: 表名
            offset: 偏移量
        """
        # 注意：检查点文件由主程序管理，这里只是接口定义
        # 实际实现应该在主程序中
        pass

    def close(self):
        """关闭所有数据库连接"""
        if self.mysql_cursor:
            self.mysql_cursor.close()
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.duckdb_conn:
            self.duckdb_conn.close()

        self.logger.info("所有数据库连接已关闭")

    def get_stats(self) -> Dict[str, int]:
        """
        获取统计信息

        Returns:
            统计信息字典
        """
        return self.stats.copy()
