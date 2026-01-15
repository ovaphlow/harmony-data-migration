#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据源读取器模块
支持多种数据源格式：SQL Server SQL 文件、XLSX、CSV、JSON
"""

import logging
import re
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any

from utils import (
    remove_brackets,
    convert_sql_server_type_to_mysql,
    quote_identifier
)


class DataReader:
    """统一的数据源读取器"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化数据读取器

        Args:
            logger: 日志记录器（可选）
        """
        self.logger = logger or logging.getLogger(__name__)

    def read_sql_server(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """
        解析 SQL Server SQL 文件，提取表结构和数据

        Args:
            file_path: SQL Server SQL 文件路径

        Returns:
            字典：表名 -> DataFrame
        """
        self.logger.info(f"开始读取 SQL Server SQL 文件: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 提取表信息和数据
            tables = self._parse_sql_server_content(content)

            self.logger.info(f"成功读取 {len(tables)} 个表")

            return tables

        except Exception as e:
            self.logger.error(f"读取 SQL Server SQL 文件失败: {e}")
            raise

    def _parse_sql_server_content(self, content: str) -> Dict[str, pd.DataFrame]:
        """
        解析 SQL Server SQL 内容

        Args:
            content: SQL 文件内容

        Returns:
            字典：表名 -> DataFrame
        """
        # 移除注释
        content = self._remove_comments(content)

        # 提取表结构
        table_schemas = self._extract_table_schemas(content)

        # 提取数据
        tables = {}
        for table_name, schema in table_schemas.items():
            data = self._extract_table_data(content, table_name)
            if data is not None and len(data) > 0:
                # 创建 DataFrame
                columns = list(schema.keys())
                df = pd.DataFrame(data, columns=columns)
                tables[table_name] = df
                self.logger.info(f"表 {table_name}: {len(df)} 行, {len(columns)} 列")

        return tables

    def _parse_mysql_content(self, content: str) -> Dict[str, pd.DataFrame]:
        """
        解析 MySQL SQL 内容

        Args:
            content: SQL 文件内容

        Returns:
            字典：表名 -> DataFrame
        """
        # 移除注释
        content = self._remove_comments(content)

        # 提取表结构
        table_schemas = self._extract_mysql_table_schemas(content)

        # 提取数据
        tables = {}
        for table_name, schema in table_schemas.items():
            data = self._extract_mysql_table_data(content, table_name)
            if data is not None and len(data) > 0:
                # 创建 DataFrame
                columns = list(schema.keys())
                df = pd.DataFrame(data, columns=columns)
                tables[table_name] = df
                self.logger.info(f"表 {table_name}: {len(df)} 行, {len(columns)} 列")

        return tables

    def _extract_mysql_table_schemas(self, content: str) -> Dict[str, Dict[str, str]]:
        """
        提取 MySQL 表结构信息

        Args:
            content: SQL 内容

        Returns:
            字典：表名 -> 列名 -> 数据类型
        """
        schemas = {}

        # 匹配 CREATE TABLE 语句（MySQL 格式，使用分号结尾）
        create_table_pattern = re.compile(
            r'CREATE TABLE\s+(?:`?(\w+)`?|\[?(\w+)\]?)(?:\s*\((.*?)\))?;\s*$',
            re.DOTALL | re.IGNORECASE
        )

        # 匹配 CREATE TABLE 语句（更完整的模式）
        create_table_pattern_full = re.compile(
            r'CREATE TABLE\s+(?:`?(\w+)`?|\[?(\w+)\]?)\s*\((.*?)\)\s*;',
            re.DOTALL | re.IGNORECASE
        )

        for match in create_table_pattern_full.finditer(content):
            # 获取表名（可能是反引号或方括号包裹的）
            table_name = match.group(1) if match.group(1) else match.group(2)
            table_body = match.group(3)

            if not table_name or not table_body:
                continue

            # 解析列定义
            columns = self._parse_mysql_columns(table_body)
            schemas[table_name] = columns

            self.logger.debug(f"提取表结构: {table_name}, {len(columns)} 列")

        return schemas

    def _parse_mysql_columns(self, table_body: str) -> Dict[str, str]:
        """
        解析 MySQL 列定义

        Args:
            table_body: CREATE TABLE 语句的括号内内容

        Returns:
            字典：列名 -> 数据类型
        """
        columns = {}

        # 分割列定义
        lines = [line.strip() for line in table_body.split('\n') if line.strip()]

        for line in lines:
            # 跳过约束定义
            if re.match(r'^(PRIMARY|UNIQUE|FOREIGN|CHECK|CONSTRAINT|INDEX|KEY)', line, re.IGNORECASE):
                continue

            # 匹配列定义
            # 格式：column_name data_type [constraints]
            # 支持：column_name 或 `column_name` 或 [column_name]
            column_match = re.match(r'^[`[]?(\w+)[]`]?\s+([\w\(\)]+)', line)
            if column_match:
                column_name = column_match.group(1)
                data_type = column_match.group(2)

                # 处理 AUTO_INCREMENT
                if 'AUTO_INCREMENT' in line:
                    data_type = re.sub(r'\s+AUTO_INCREMENT', '', data_type, flags=re.IGNORECASE)
                    # 标记为自增列
                    data_type += ' AUTO_INCREMENT'

                # 转换数据类型（已经是 MySQL 格式，简单处理即可）
                data_type = convert_sql_server_type_to_mysql(data_type)

                columns[column_name] = data_type

        return columns

    def _extract_mysql_table_data(self, content: str, table_name: str) -> Optional[List[List[Any]]]:
        """
        提取 MySQL 表数据

        Args:
            content: SQL 内容
            table_name: 表名

        Returns:
            数据列表（每行一个列表），如果没有数据则返回 None
        """
        # 构建匹配模式
        # MySQL INSERT 格式：INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...), (val1, val2, ...), ...;
        # 或者单行 INSERT：INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...);

        # 匹配所有 INSERT 语句
        pattern = re.compile(
            rf'INSERT\s+INTO\s+(?:`?{re.escape(table_name)}`?|\[?{re.escape(table_name)}\]?)\s*\((.*?)\)\s+VALUES\s+([^;]+);',
            re.DOTALL | re.IGNORECASE
        )

        all_values = []
        first_match = True
        columns_part = None

        for match in pattern.finditer(content):
            if first_match:
                columns_part = match.group(1)
                first_match = False

            values_part = match.group(2)

            # 解析当前 INSERT 语句中的值
            current_values = self._parse_values(values_part)
            all_values.extend(current_values)

        if not all_values:
            return None

        # 解析列名
        columns = [col.strip().strip('[]`') for col in columns_part.split(',')]

        return all_values

    def _remove_comments(self, content: str) -> str:
        """
        移除 SQL 注释

        Args:
            content: SQL 内容

        Returns:
            处理后的 SQL 内容
        """
        # 移除多行注释 /* ... */
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

        # 移除单行注释 --
        content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)

        return content

    def _extract_table_schemas(self, content: str) -> Dict[str, Dict[str, str]]:
        """
        提取表结构信息

        Args:
            content: SQL 内容

        Returns:
            字典：表名 -> 列名 -> 数据类型
        """
        schemas = {}

        # 匹配 CREATE TABLE 语句
        create_table_pattern = re.compile(
            r'CREATE TABLE\s+(\[?[\w.]*?\]?\.?\[?(\w+)\]?)\s*\((.*?)\)\s*;',
            re.DOTALL | re.IGNORECASE
        )

        for match in create_table_pattern.finditer(content):
            table_name = match.group(2)
            table_body = match.group(3)

            # 解析列定义
            columns = self._parse_columns(table_body)
            schemas[table_name] = columns

            self.logger.debug(f"提取表结构: {table_name}, {len(columns)} 列")

        return schemas

    def _parse_columns(self, table_body: str) -> Dict[str, str]:
        """
        解析列定义

        Args:
            table_body: CREATE TABLE 语句的括号内内容

        Returns:
            字典：列名 -> 数据类型
        """
        columns = {}

        # 分割列定义
        lines = [line.strip() for line in table_body.split('\n') if line.strip()]

        for line in lines:
            # 跳过约束定义
            if re.match(r'^(PRIMARY|UNIQUE|FOREIGN|CHECK|CONSTRAINT|INDEX)', line, re.IGNORECASE):
                continue

            # 匹配列定义
            # 格式：column_name data_type [constraints]
            column_match = re.match(r'^\[?(\w+)\]?\s+([\w\(\)]+)', line)
            if column_match:
                column_name = column_match.group(1)
                data_type = column_match.group(2)

                # 处理 IDENTITY
                if 'IDENTITY' in line:
                    data_type = re.sub(r'\s+IDENTITY\([^)]+\)', '', data_type, flags=re.IGNORECASE)
                    # 标记为自增列
                    data_type += ' AUTO_INCREMENT'

                # 转换数据类型
                data_type = convert_sql_server_type_to_mysql(data_type)

                columns[column_name] = data_type

        return columns

    def _extract_table_data(self, content: str, table_name: str) -> Optional[List[List[Any]]]:
        """
        提取表数据

        Args:
            content: SQL 内容
            table_name: 表名

        Returns:
            数据列表（每行一个列表），如果没有数据则返回 None
        """
        # 构建匹配模式
        # INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...), (val1, val2, ...), ...;
        pattern = re.compile(
            rf'INSERT\s+INTO\s+[\w.]*?\[?{re.escape(table_name)}\]?\s*\((.*?)\)\s+VALUES\s+([^;]+);',
            re.DOTALL | re.IGNORECASE
        )

        match = pattern.search(content)
        if not match:
            return None

        columns_part = match.group(1)
        values_part = match.group(2)

        # 解析列名
        columns = [col.strip().strip('[]') for col in columns_part.split(',')]

        # 解析值
        values = self._parse_values(values_part)

        return values

    def _parse_values(self, values_part: str) -> List[List[Any]]:
        """
        解析 VALUES 子句中的值

        Args:
            values_part: VALUES 子句内容

        Returns:
            数据列表（每行一个列表）
        """
        values = []

        # 分割多个值组
        # 使用正则表达式匹配 (val1, val2, ...)
        value_groups = re.findall(r'\((.*?)\)(?:,\s*|\s*$)', values_part, re.DOTALL)

        for group in value_groups:
            # 解析单个值组中的值
            row_values = self._parse_single_row(group)
            values.append(row_values)

        return values

    def _parse_single_row(self, row_str: str) -> List[Any]:
        """
        解析单行数据

        Args:
            row_str: 单行数据字符串

        Returns:
            值列表
        """
        values = []
        current_value = ''
        in_string = False
        string_char = None
        escape_next = False

        i = 0
        while i < len(row_str):
            char = row_str[i]

            # 处理转义字符
            if char == '\\' and in_string and not escape_next:
                escape_next = True
                current_value += char
                i += 1
                continue

            # 处理字符串
            if char in ("'", '"') and not escape_next:
                if not in_string:
                    in_string = True
                    string_char = char
                    current_value += char
                elif char == string_char:
                    in_string = False
                    string_char = None
                    current_value += char
                else:
                    current_value += char
                i += 1
                continue

            # 处理分隔符
            if char == ',' and not in_string:
                # 完成当前值
                values.append(self._parse_value(current_value.strip()))
                current_value = ''
            else:
                current_value += char

            escape_next = False
            i += 1

        # 添加最后一个值
        if current_value.strip():
            values.append(self._parse_value(current_value.strip()))

        return values

    def _parse_value(self, value_str: str) -> Any:
        """
        解析单个值

        Args:
            value_str: 值字符串

        Returns:
            解析后的值（字符串、数字或 NULL）
        """
        value_str = value_str.strip()

        # NULL 值
        if value_str.upper() == 'NULL':
            return None

        # 字符串值
        if value_str.startswith("'") or value_str.startswith('"'):
            # 移除引号
            value_str = value_str[1:-1]
            # 处理转义字符
            value_str = value_str.replace("''", "'").replace('\\"', '"').replace("\\'", "'")
            return value_str

        # 数值
        if re.match(r'^-?\d+$', value_str):
            return int(value_str)
        if re.match(r'^-?\d+\.\d+$', value_str):
            return float(value_str)

        # 默认返回字符串
        return value_str

    def read_mysql_sql(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """
        读取 MySQL 格式的 SQL 文件

        Args:
            file_path: MySQL SQL 文件路径

        Returns:
            字典：表名 -> DataFrame
        """
        self.logger.info(f"开始读取 MySQL SQL 文件: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 提取表信息和数据
            tables = self._parse_mysql_content(content)

            self.logger.info(f"成功读取 {len(tables)} 个表")

            return tables

        except Exception as e:
            self.logger.error(f"读取 MySQL SQL 文件失败: {e}")
            raise

    def read_excel(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """
        读取 XLSX 文件

        Args:
            file_path: XLSX 文件路径

        Returns:
            字典：工作表名 -> DataFrame
        """
        self.logger.info(f"开始读取 XLSX 文件: {file_path}")

        try:
            # 读取所有工作表
            excel_file = pd.ExcelFile(file_path)
            tables = {}

            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                tables[sheet_name] = df
                self.logger.info(f"工作表 {sheet_name}: {len(df)} 行, {len(df.columns)} 列")

            self.logger.info(f"成功读取 {len(tables)} 个工作表")
            return tables

        except Exception as e:
            self.logger.error(f"读取 XLSX 文件失败: {e}")
            raise

    def read_csv(self, file_path: str, table_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        读取 CSV 文件

        Args:
            file_path: CSV 文件路径
            table_name: 表名（可选，默认使用文件名）

        Returns:
            字典：表名 -> DataFrame
        """
        self.logger.info(f"开始读取 CSV 文件: {file_path}")

        try:
            df = pd.read_csv(file_path)

            if table_name is None:
                # 使用文件名（不含扩展名）作为表名
                import os
                table_name = os.path.splitext(os.path.basename(file_path))[0]

            self.logger.info(f"成功读取 CSV: {len(df)} 行, {len(df.columns)} 列")

            return {table_name: df}

        except Exception as e:
            self.logger.error(f"读取 CSV 文件失败: {e}")
            raise

    def read_json(self, file_path: str, table_name: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        读取 JSON 文件

        Args:
            file_path: JSON 文件路径
            table_name: 表名（可选，默认使用文件名）

        Returns:
            字典：表名 -> DataFrame
        """
        self.logger.info(f"开始读取 JSON 文件: {file_path}")

        try:
            df = pd.read_json(file_path)

            if table_name is None:
                # 使用文件名（不含扩展名）作为表名
                import os
                table_name = os.path.splitext(os.path.basename(file_path))[0]

            self.logger.info(f"成功读取 JSON: {len(df)} 行, {len(df.columns)} 列")

            return {table_name: df}

        except Exception as e:
            self.logger.error(f"读取 JSON 文件失败: {e}")
            raise
