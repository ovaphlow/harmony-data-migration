#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Server到MySQL语法转换工具

功能说明：
- 将SQL Server的CREATE TABLE语句转换为MySQL兼容格式
- 处理IDENTITY列转换为AUTO_INCREMENT
- 移除方括号标识符
- 移除COLLATE子句
- 转换数据类型
- 添加主键约束

使用方法：
    python3 sql_server_to_mysql.py <输入文件路径>

输出文件：
    在target-data目录下生成 <原文件名>_mysql.sql

支持的转换：
- IDENTITY(1,1) → AUTO_INCREMENT
- [column_name] → column_name
- COLLATE Chinese_PRC_CI_AS → (移除)
- numeric(18,2) → decimal(18,2)
- 添加PRIMARY KEY约束
"""

import argparse
import os
import re
import logging
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 预编译常用正则表达式
# 注释处理
MULTILINE_COMMENT_PATTERN = re.compile(r'\/\*.*?\*\/', re.DOTALL)
SINGLELINE_COMMENT_PATTERN = re.compile(r'--.*$', re.MULTILINE)
QUOTED_PATTERN = re.compile(r"'[^']*'")

# DROP TABLE语句
DROP_TABLE_PATTERN1 = re.compile(r'IF EXISTS \(SELECT \* FROM sys\.all_objects WHERE object_id = OBJECT_ID\(N\'\[dbo\]\.\[(\w+)\]\'\) AND type IN \(\'U\'\)\)\s+DROP TABLE \[dbo\]\.\[\1\]')
DROP_TABLE_PATTERN2 = re.compile(r'IF EXISTS \(SELECT \* FROM sys\.all_objects WHERE object_id = OBJECT_ID\(N\'(\w+)\'\) AND type IN \(\'U\'\)\)\s+DROP TABLE \1')

# IDENTITY_INSERT语句
IDENTITY_INSERT_ON_PATTERNS = [
    re.compile(r'SET\s+IDENTITY_INSERT\s+\[.*?\]\.\[.*?\]\s+ON', re.IGNORECASE),
    re.compile(r'SET\s+IDENTITY_INSERT\s+\[.*?\]\s+ON', re.IGNORECASE),
    re.compile(r'SET\s+IDENTITY_INSERT\s+.*?\s+ON', re.IGNORECASE)
]
IDENTITY_INSERT_OFF_PATTERNS = [
    re.compile(r'SET\s+IDENTITY_INSERT\s+\[.*?\]\.\[.*?\]\s+OFF', re.IGNORECASE),
    re.compile(r'SET\s+IDENTITY_INSERT\s+\[.*?\]\s+OFF', re.IGNORECASE),
    re.compile(r'SET\s+IDENTITY_INSERT\s+.*?\s+OFF', re.IGNORECASE)
]

# ALTER TABLE语句
ALTER_TABLE_LOCK_ESCALATION_PATTERNS = [
    re.compile(r'ALTER\s+TABLE\s+\[.*?\]\.\[.*?\]\s+SET\s+\(LOCK_ESCALATION\s*=\s*TABLE\)', re.IGNORECASE),
    re.compile(r'ALTER\s+TABLE\s+\[.*?\]\s+SET\s+\(LOCK_ESCALATION\s*=\s*TABLE\)', re.IGNORECASE),
    re.compile(r'ALTER\s+TABLE\s+.*?\s+SET\s+\(LOCK_ESCALATION\s*=\s*TABLE\)', re.IGNORECASE)
]
ALTER_TABLE_PRIMARY_KEY_PATTERN = re.compile(r'ALTER\s+TABLE\s+(.+?)\s+ADD\s+CONSTRAINT\s+(.+?)\s+PRIMARY\s+KEY.*?WITH\s*\([^)]+\)\s+ON\s+PRIMARY', re.IGNORECASE | re.DOTALL)
WITH_ON_PRIMARY_PATTERN = re.compile(r'\s+WITH\s*\([^)]+\)\s+ON\s+PRIMARY\s*', re.IGNORECASE)
WITH_ON_PRIMARY_PATTERN2 = re.compile(r'\s*WITH\s*\([^)]*\)\s*ON\s+PRIMARY\s*', re.IGNORECASE | re.DOTALL)

# 主键注释
PRIMARY_KEY_COMMENT_PATTERN = re.compile(r'\s*--\s*Primary\s+Key\s+structure\s+for\s+table.*?\n', re.IGNORECASE)
COMMENT_SEPARATOR_PATTERN = re.compile(r'\s*--\s*----------------------------\s*--\s*----------------------------\s*\n', re.IGNORECASE)

# 复杂的主键定义
COMPLEX_PRIMARY_KEY_PATTERN1 = re.compile(r'\s*ALTER\s+TABLE\s+.*?ADD\s+CONSTRAINT\s+.*?PRIMARY\s+KEY\s+NONCLUSTERED\s*\([^)]+\)\s*\n\s*WITH\s*\([^)]+\)\s*\n\s*ON\s+PRIMARY\s*\n?', re.IGNORECASE | re.DOTALL)
COMPLEX_PRIMARY_KEY_PATTERN2 = re.compile(r'\s*--\s*----------------------------\s*\n\s*--\s*Primary\s+Key\s+structure\s+for\s+table\s+.*?\n\s*--\s*----------------------------\s*\n\s*ALTER\s+TABLE\s+.*?ADD\s+CONSTRAINT\s+.*?PRIMARY\s+KEY\s+.*?\n\s*WITH\s*\([^)]+\)\s*\n\s*ON\s+PRIMARY\s*\n?', re.IGNORECASE | re.DOTALL)
COMPLEX_PRIMARY_KEY_PATTERN3 = re.compile(r'ALTER\s+TABLE\s+.*?ADD\s+CONSTRAINT\s+.*?PRIMARY\s+KEY\s+NONCLUSTERED\s*\([^)]+\)\s*\n\s*WITH\s*\([^)]+\)\s*\n\s*ON\s+PRIMARY\s*\n', re.IGNORECASE)
COMPLEX_PRIMARY_KEY_PATTERN4 = re.compile(r'\n\s*ALTER\s+TABLE\s+.*?ADD\s+CONSTRAINT\s+.*?PRIMARY\s+KEY\s+.*?\n\s*WITH\s*\([^)]+\)\s*\n\s*ON\s+PRIMARY\s*$', re.IGNORECASE)

# 列定义和数据类型
BRACKET_PATTERN = re.compile(r'\[([^\]]+)\]')
COLLATE_PATTERN = re.compile(r'\s+COLLATE\s+Chinese_PRC_CI_AS', re.IGNORECASE)
COLLATE_GENERAL_PATTERN = re.compile(r'\s+COLLATE\s+[^\s,]+', re.IGNORECASE)
DBO_PREFIX_PATTERN = re.compile(r'\bdbo\.')
UNICODE_STRING_PATTERN = re.compile(r"N'([^']*)'")
NUMERIC_TO_DECIMAL_PATTERN = re.compile(r'\bnumeric\b', re.IGNORECASE)
GO_STATEMENT_PATTERN = re.compile(r'\bGO\b')
MONEY_TO_DECIMAL_PATTERN = re.compile(r'\bmoney\b', re.IGNORECASE)
MONEY_PATTERN = re.compile(r'\bmoney\b', re.IGNORECASE)
IDENTITY_PATTERN = re.compile(r'\s+IDENTITY\([^)]+\)', re.IGNORECASE)
AUTO_INCREMENT_PATTERN = re.compile(r'(\[?\w+\]?)\s+int\s+(NOT\s+NULL|NULL)', re.IGNORECASE)
PRIMARY_KEY_COL_PATTERN = re.compile(r'(\[?\w+\]?)\s+int\s+AUTO_INCREMENT\s+NOT\s+NULL', re.IGNORECASE)

# CREATE TABLE语句
CREATE_TABLE_PATTERN = re.compile(r'CREATE TABLE\s+(\[?dbo\]?\.)?\[?(\w+)\]?\s*\((.*?)\)\s*;', re.DOTALL | re.IGNORECASE)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='将SQL Server导出的SQL文件转换为MySQL语法')
    parser.add_argument('sql_file', help='要转换的SQL文件路径')
    return parser.parse_args()


def convert_sql_server_to_mysql(sql_content):
    """
    将SQL Server语法的SQL转换为MySQL语法
    
    Args:
        sql_content: 原始SQL Server格式的SQL内容
        
    Returns:
        转换后的MySQL格式的SQL内容
    """
    # 记录转换开始时间，用于性能分析
    start_time = time.time()
    logger.info("开始SQL Server到MySQL语法转换...")
    
    # 记录原始内容大小
    logger.info(f"原始SQL内容大小: {len(sql_content)} 字符")
    
    # 记录处理步骤
    logger.info("步骤 1: 处理多行和单行注释...")
    
    # 移除所有多行注释 /* ... */
    # sql_content = MULTILINE_COMMENT_PATTERN.sub('', sql_content)
    
    # 移除所有单行注释 --
    # 注意：需要考虑引号内的注释符号不应被移除
    # 先处理引号内的内容，暂时替换掉，处理完注释后再恢复
    quoted_matches = []
    
    def replace_quoted(match):
        quoted_matches.append(match.group(0))
        return f"__QUOTED_PLACEHOLDER_{len(quoted_matches) - 1}__"
    
    # 替换所有引号内的内容为占位符
    # sql_content = QUOTED_PATTERN.sub(replace_quoted, sql_content)
    
    # 移除所有单行注释
    sql_content = SINGLELINE_COMMENT_PATTERN.sub('', sql_content)
    
    # 恢复引号内的内容
    for i, quoted_text in enumerate(quoted_matches):
        sql_content = sql_content.replace(f"__QUOTED_PLACEHOLDER_{i}__", quoted_text)
    
    logger.info("完成注释处理")
    
    # 记录处理步骤
    logger.info("步骤 2: 转换DROP TABLE语句...")
    
    # 1. 转换DROP TABLE语句
    sql_content = DROP_TABLE_PATTERN1.sub(r'DROP TABLE IF EXISTS \1', sql_content)
    sql_content = DROP_TABLE_PATTERN2.sub(r'DROP TABLE IF EXISTS \1', sql_content)
    
    logger.info("完成DROP TABLE语句转换")
    
    # 记录处理步骤
    logger.info("步骤 3: 转换IDENTITY_INSERT语句...")
    
    # 2. 转换SET IDENTITY_INSERT语句为MySQL语法
    for pattern in IDENTITY_INSERT_ON_PATTERNS:
        sql_content = pattern.sub('SET FOREIGN_KEY_CHECKS=0;', sql_content)
    
    for pattern in IDENTITY_INSERT_OFF_PATTERNS:
        sql_content = pattern.sub('SET FOREIGN_KEY_CHECKS=1;', sql_content)
    
    logger.info("完成IDENTITY_INSERT语句转换")
    
    # 记录处理步骤
    logger.info("步骤 4: 处理ALTER TABLE和其他语句...")
    
    # 3. 移除ALTER TABLE SET LOCK_ESCALATION语句
    for pattern in ALTER_TABLE_LOCK_ESCALATION_PATTERNS:
        sql_content = pattern.sub('', sql_content)
    
    logger.info("完成ALTER TABLE SET LOCK_ESCALATION语句处理")
    
    # 记录处理步骤
    logger.info("步骤 5: 移除COLLATE子句和dbo.前缀...")
    
    # 移除COLLATE子句和删除dbo.前缀 - 合并操作
    sql_content = COLLATE_PATTERN.sub('', sql_content)
    sql_content = DBO_PREFIX_PATTERN.sub('', sql_content)
    
    logger.info("完成COLLATE子句和dbo.前缀处理")
    
    # 记录处理步骤
    logger.info("步骤 6: 处理字符串格式...")
    
    # 转换N''字符串为MySQL格式
    sql_content = UNICODE_STRING_PATTERN.sub(lambda m: "'" + m.group(1) + "'", sql_content)
    
    logger.info("完成字符串格式转换")
    
    # 记录处理步骤
    logger.info("步骤 7: 标识符处理...")
    
    # 在这里不要移除方括号，让CREATE TABLE处理函数来统一处理
    # sql_content = BRACKET_PATTERN.sub(r'\1', sql_content)
    
    logger.info("完成标识符处理准备")
    
    # 记录处理步骤
    logger.info("步骤 8: 转换数据类型...")
    
    # 转换数据类型：numeric、money等
    sql_content = NUMERIC_TO_DECIMAL_PATTERN.sub('decimal', sql_content)
    sql_content = MONEY_TO_DECIMAL_PATTERN.sub('decimal(19,4)', sql_content)
    
    logger.info("完成数据类型转换")
    
    # 记录处理步骤
    logger.info("步骤 9: 将GO语句替换为分号...")
    
    # 将GO语句替换为分号
    sql_content = GO_STATEMENT_PATTERN.sub(';', sql_content)
    
    logger.info("完成GO语句转换")
    
    # 记录处理步骤
    logger.info("步骤 10: 处理INSERT数据...")
    
    # 获取原始行数，用于进度日志
    total_lines = len(sql_content.split('\n'))
    
    # 4. 处理包含INSERT数据的完整文件
    lines = sql_content.split('\n')
    processed_lines = []
    skip_everything = False
    
    for i, line in enumerate(lines):
        line = line.rstrip()
        
        # 每处理100行输出一次进度日志
        if i % 100 == 0:
            progress_percent = (i / total_lines) * 100
            logger.info(f"处理行进度: {i}/{total_lines} 行 ({progress_percent:.1f}%)")
        
        # 如果已经决定跳过所有内容，跳过
        if skip_everything:
            continue
        
        # 跳过SQL Server特有注释
        if '-- ----------------------------' in line or '-- Records of' in line:
            continue
        
        # 检查是否到了主键约束定义区域，从这里开始跳过所有内容
        if ('ALTER TABLE' in line and 'ADD CONSTRAINT' in line and 'PRIMARY KEY' in line) or 'Primary Key structure' in line:
            logger.info(f"跳过主键约束定义部分，行 {i}")
            skip_everything = True
            continue
        
        # 组合优化：一次性完成多个字符串操作
        # 移除方括号、COLLATE子句、dbo.前缀、转换N''字符串和移除不支持的语句
        processed_line = line
        processed_line = BRACKET_PATTERN.sub(r'\1', processed_line)
        processed_line = COLLATE_PATTERN.sub('', processed_line)
        processed_line = DBO_PREFIX_PATTERN.sub('', processed_line)
        processed_line = UNICODE_STRING_PATTERN.sub(lambda m: "'" + m.group(1) + "'", processed_line)
        processed_line = NUMERIC_TO_DECIMAL_PATTERN.sub('decimal', processed_line)
        processed_line = MONEY_TO_DECIMAL_PATTERN.sub('decimal(19,4)', processed_line)
        
        # 在INSERT数据处理步骤也移除ALTER TABLE SET LOCK_ESCALATION
        if re.search(r'ALTER TABLE.*SET\s+LOCK_ESCALATION', processed_line, re.IGNORECASE):
            processed_line = ''
        
        # 保留非空的处理后行
        if processed_line.strip() or not line.strip():
            processed_lines.append(processed_line if processed_line.strip() == '' else processed_line)
    
    logger.info("完成INSERT数据处理")
    
    # 重建SQL内容
    sql_content = '\n'.join(processed_lines)
    
    # 记录处理步骤
    logger.info("步骤 11: 处理CREATE TABLE语句...")
    
    # 5. 更精确地处理CREATE TABLE语句
    # 使用预编译的正则表达式匹配CREATE TABLE语句
    create_table_pattern = re.compile(r'CREATE TABLE\s+(\[?dbo\]?\.)?\[?(\w+)\]?\s*\((.*?)\)\s*;', re.DOTALL | re.IGNORECASE)
    
    def process_create_table(match):
        # 获取表名（去掉可能的dbo.前缀）
        table_name = match.group(2)
        table_body = match.group(3)
        
        logger.info(f"处理CREATE TABLE语句: {table_name}")
        
        # 处理列定义
        lines = table_body.split('\n')
        processed_lines = []
        primary_key_col = None
        
        for i, line in enumerate(lines):
            original_line = line
            line = line.strip()
            if not line:
                continue
            
            # 完全移除注释部分
            sql_part = line
            if '--' in line:
                sql_part = line.split('--')[0].rstrip()
                # 如果移除注释后内容为空，则跳过此行
                if not sql_part.strip():
                    continue
            
            # 处理IDENTITY列（在处理方括号之前）
            if 'IDENTITY(' in sql_part:
                # 移除IDENTITY关键字并添加AUTO_INCREMENT
                sql_part = IDENTITY_PATTERN.sub('', sql_part)
                # 添加AUTO_INCREMENT到int列定义
                sql_part = AUTO_INCREMENT_PATTERN.sub(r'\1 int AUTO_INCREMENT \2', sql_part)
                # 记录主键列名
                pk_match = PRIMARY_KEY_COL_PATTERN.search(sql_part)
                if pk_match:
                    primary_key_col = pk_match.group(1)
            
            # 合并多个字符串操作：移除方括号、COLLATE子句和转换数据类型
            sql_part = BRACKET_PATTERN.sub(r'\1', sql_part)
            sql_part = COLLATE_PATTERN.sub('', sql_part)
            sql_part = MONEY_TO_DECIMAL_PATTERN.sub('decimal(19,4)', sql_part)
            
            # 移除尾随逗号
            sql_part = sql_part.rstrip(',')
            processed_lines.append(sql_part)
        
        # 重建表定义，但现在需要正确处理逗号
        # 确保每一行都有逗号，除了最后一行
        if processed_lines:
            # 为除了最后一行的所有行添加逗号
            for i in range(len(processed_lines) - 1):
                if processed_lines[i].strip():
                    processed_lines[i] += ','
            
        new_table_body = '\n  '.join(processed_lines)
        
        # 添加PRIMARY KEY约束（如果有AUTO_INCREMENT列）
        if primary_key_col and 'PRIMARY KEY' not in new_table_body:
            # 移除主键列名中的方括号
            clean_pk_col = primary_key_col.strip('[]')
            
            # 在追加PRIMARY KEY之前，确保最后一行没有多余逗号
            new_table_body = new_table_body.rstrip(',').rstrip()
            
            # 添加主键约束
            new_table_body += f',\n  PRIMARY KEY ({clean_pk_col})'
        
        logger.info(f"完成表 {table_name} 的CREATE TABLE处理")
        
        # 重建CREATE TABLE语句
        return f'CREATE TABLE {table_name} (\n  {new_table_body}\n)'
    
    # 替换所有CREATE TABLE语句
    sql_content = create_table_pattern.sub(process_create_table, sql_content)
    
    logger.info("完成CREATE TABLE语句处理")
    
    # 6. 清理多余的空行
    sql_content = re.sub(r'\n\s*\n', '\n\n', sql_content)
    
    return sql_content


def validate_input_file(file_path):
    """验证输入文件的有效性"""
    if not file_path:
        raise ValueError("文件路径不能为空")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件 '{file_path}' 不存在")
    
    if not os.path.isfile(file_path):
        raise ValueError(f"'{file_path}' 不是文件")
    
    if os.path.getsize(file_path) == 0:
        raise ValueError(f"文件 '{file_path}' 为空")
    
    # 检查文件扩展名
    valid_extensions = ['.sql', '.SQL']
    if not any(file_path.lower().endswith(ext) for ext in valid_extensions):
        print(f"警告: 文件扩展名不是标准的 .sql，但继续处理...")

def main():
    """主函数"""
    args = parse_arguments()
    sql_file = args.sql_file
    
    try:
        # 验证输入文件
        validate_input_file(sql_file)
        
        # 读取文件内容
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        print(f"成功读取文件: {sql_file}")
        print(f"文件大小: {os.path.getsize(sql_file)} 字节")
        
    except FileNotFoundError as e:
        print(f"错误: {e}")
        return 1
    except ValueError as e:
        print(f"错误: {e}")
        return 1
    except UnicodeDecodeError as e:
        print(f"错误: 文件编码问题 - {e}")
        print("请确保文件使用UTF-8编码")
        return 1
    except Exception as e:
        print(f"错误: 读取文件时出错: {e}")
        return 1
    
    # 转换SQL语法
    print("开始转换SQL语法...")
    try:
        mysql_sql_content = convert_sql_server_to_mysql(sql_content)
        if not mysql_sql_content.strip():
            print("警告: 转换后的内容为空")
    except Exception as e:
        print(f"错误: 转换过程失败 - {e}")
        return 1
    
    # 生成输出文件名 - 保存到target-data目录
    # 确保target-data目录存在
    target_dir = os.path.join(os.path.dirname(os.path.dirname(sql_file)), 'target-data')
    os.makedirs(target_dir, exist_ok=True)
    
    # 从输入文件名中提取基础名称
    base_name = os.path.basename(sql_file)
    name_without_ext = os.path.splitext(base_name)[0]
    ext = os.path.splitext(base_name)[1]
    
    # 生成目标文件名
    output_file = os.path.join(target_dir, f"{name_without_ext}_mysql{ext}")
    
    # 保存转换后的SQL文件
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(mysql_sql_content)
        
        print(f"成功保存转换后的文件: {output_file}")
        print(f"输出文件大小: {os.path.getsize(output_file)} 字节")
        
        # 显示转换统计信息
        original_lines = len(sql_content.split('\n'))
        converted_lines = len(mysql_sql_content.split('\n'))
        print(f"转换统计: {original_lines} 行 → {converted_lines} 行")
        
    except Exception as e:
        print(f"错误: 保存文件时出错: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
