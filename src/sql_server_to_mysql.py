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
    # 1. 转换DROP TABLE语句
    sql_content = re.sub(
        r'IF EXISTS \(SELECT \* FROM sys\.all_objects WHERE object_id = OBJECT_ID\(N\'\[dbo\]\.\[(\w+)\]\'\) AND type IN \(\'U\'\)\)\s+DROP TABLE \[dbo\]\.\[\1\]',
        r'DROP TABLE IF EXISTS \1',
        sql_content
    )
    sql_content = re.sub(
        r'IF EXISTS \(SELECT \* FROM sys\.all_objects WHERE object_id = OBJECT_ID\(N\'(\w+)\'\) AND type IN \(\'U\'\)\)\s+DROP TABLE \1',
        r'DROP TABLE IF EXISTS \1',
        sql_content
    )
    
    # 2. 转换SET IDENTITY_INSERT语句为MySQL语法
    sql_content = re.sub(r'SET\s+IDENTITY_INSERT\s+\[.*?\]\.\[.*?\]\s+ON', 'SET FOREIGN_KEY_CHECKS=0;', sql_content, flags=re.IGNORECASE)
    sql_content = re.sub(r'SET\s+IDENTITY_INSERT\s+\[.*?\]\s+ON', 'SET FOREIGN_KEY_CHECKS=0;', sql_content, flags=re.IGNORECASE)
    sql_content = re.sub(r'SET\s+IDENTITY_INSERT\s+.*?\s+ON', 'SET FOREIGN_KEY_CHECKS=0;', sql_content, flags=re.IGNORECASE)
    
    sql_content = re.sub(r'SET\s+IDENTITY_INSERT\s+\[.*?\]\.\[.*?\]\s+OFF', 'SET FOREIGN_KEY_CHECKS=1;', sql_content, flags=re.IGNORECASE)
    sql_content = re.sub(r'SET\s+IDENTITY_INSERT\s+\[.*?\]\s+OFF', 'SET FOREIGN_KEY_CHECKS=1;', sql_content, flags=re.IGNORECASE)
    sql_content = re.sub(r'SET\s+IDENTITY_INSERT\s+.*?\s+OFF', 'SET FOREIGN_KEY_CHECKS=1;', sql_content, flags=re.IGNORECASE)
    
    # 3. 移除ALTER TABLE SET LOCK_ESCALATION语句
    sql_content = re.sub(r'ALTER\s+TABLE\s+\[.*?\]\.\[.*?\]\s+SET\s+\(LOCK_ESCALATION\s*=\s*TABLE\)', '', sql_content, flags=re.IGNORECASE)
    sql_content = re.sub(r'ALTER\s+TABLE\s+\[.*?\]\s+SET\s+\(LOCK_ESCALATION\s*=\s*TABLE\)', '', sql_content, flags=re.IGNORECASE)
    sql_content = re.sub(r'ALTER\s+TABLE\s+.*?\s+SET\s+\(LOCK_ESCALATION\s*=\s*TABLE\)', '', sql_content, flags=re.IGNORECASE)
    
    # 4. 处理ALTER TABLE ADD CONSTRAINT语句，移除SQL Server特有的WITH子句和ON PRIMARY
    sql_content = re.sub(
        r'ALTER\s+TABLE\s+(.+?)\s+ADD\s+CONSTRAINT\s+(.+?)\s+PRIMARY\s+KEY.*?WITH\s*\([^)]+\)\s+ON\s+PRIMARY',
        r'ALTER TABLE \1 ADD PRIMARY KEY',
        sql_content,
        flags=re.IGNORECASE | re.DOTALL
    )
    
    # 更精确地处理WITH子句和ON PRIMARY部分（针对复杂的PRIMARY KEY定义）
    sql_content = re.sub(
        r'\s+WITH\s*\([^)]+\)\s+ON\s+PRIMARY\s*',
        '',
        sql_content,
        flags=re.IGNORECASE
    )
    
    # 移除主键注释语句
    sql_content = re.sub(r'\s*--\s*Primary\s+Key\s+structure\s+for\s+table.*?\n', '\n', sql_content, flags=re.IGNORECASE)
    
    # 移除更复杂的WITH子句和ON PRIMARY结构
    sql_content = re.sub(
        r'\s*WITH\s*\([^)]*\)\s*ON\s+PRIMARY\s*',
        '',
        sql_content,
        flags=re.IGNORECASE | re.DOTALL
    )
    
    # 移除SQL Server特有的主键注释分隔符
    sql_content = re.sub(r'\s*--\s*----------------------------\s*--\s*----------------------------\s*\n', '\n', sql_content, flags=re.IGNORECASE)
    
    # 移除完整的ALTER TABLE ADD CONSTRAINT语句（包括WITH和ON PRIMARY）
    sql_content = re.sub(
        r'\s*ALTER\s+TABLE\s+.*?ADD\s+CONSTRAINT\s+.*?PRIMARY\s+KEY\s+NONCLUSTERED\s*\([^)]+\)\s*\n\s*WITH\s*\([^)]+\)\s*\n\s*ON\s+PRIMARY\s*\n?',
        '',
        sql_content,
        flags=re.IGNORECASE | re.DOTALL
    )
    
    # 移除最后的多行主键定义语句（包含主键结构注释）
    sql_content = re.sub(
        r'\s*--\s*----------------------------\s*\n\s*--\s*Primary\s+Key\s+structure\s+for\s+table\s+.*?\n\s*--\s*----------------------------\s*\n\s*ALTER\s+TABLE\s+.*?ADD\s+CONSTRAINT\s+.*?PRIMARY\s+KEY\s+.*?\n\s*WITH\s*\([^)]+\)\s*\n\s*ON\s+PRIMARY\s*\n?',
        '',
        sql_content,
        flags=re.IGNORECASE | re.DOTALL
    )
    
    # 移除独立的ALTER TABLE PRIMARY KEY语句（多行格式）
    sql_content = re.sub(
        r'ALTER\s+TABLE\s+.*?ADD\s+CONSTRAINT\s+.*?PRIMARY\s+KEY\s+NONCLUSTERED\s*\([^)]+\)\s*\n\s*WITH\s*\([^)]+\)\s*\n\s*ON\s+PRIMARY\s*\n',
        '',
        sql_content,
        flags=re.IGNORECASE
    )
    
    # 移除文件末尾的ALTER TABLE语句（确保文件结尾干净）
    sql_content = re.sub(
        r'\n\s*ALTER\s+TABLE\s+.*?ADD\s+CONSTRAINT\s+.*?PRIMARY\s+KEY\s+.*?\n\s*WITH\s*\([^)]+\)\s*\n\s*ON\s+PRIMARY\s*$',
        '',
        sql_content,
        flags=re.IGNORECASE
    )
    
    # 移除SQL Server特有的COLLATE子句
    sql_content = re.sub(r'\s+COLLATE\s+Chinese_PRC_CI_AS', '', sql_content, flags=re.IGNORECASE)
    # 移除方括号标识符
    sql_content = re.sub(r'\[([^\]]+)\]', r'\1', sql_content)
    # 转换numeric为decimal
    sql_content = re.sub(r'\bnumeric\b', 'decimal', sql_content, flags=re.IGNORECASE)
    
    # 3. 处理GO语句：将GO语句替换为分号
    sql_content = re.sub(r'\bGO\b', ';', sql_content)
    
    # 4. 处理包含INSERT数据的完整文件
    lines = sql_content.split('\n')
    processed_lines = []
    skip_everything = False
    
    for i, line in enumerate(lines):
        line = line.rstrip()
        
        # 如果已经决定跳过所有内容，跳过
        if skip_everything:
            continue
        
        # 跳过SQL Server特有注释
        if '-- ----------------------------' in line or '-- Records of' in line:
            continue
        
        # 检查是否到了主键约束定义区域，从这里开始跳过所有内容
        if ('ALTER TABLE' in line and 'ADD CONSTRAINT' in line and 'PRIMARY KEY' in line) or 'Primary Key structure' in line:
            skip_everything = True
            continue
        
        # 处理当前行
        processed_line = line
        
        # 移除方括号
        processed_line = re.sub(r'\[([^\]]+)\]', r'\1', processed_line)
        
        # 移除COLLATE Chinese_PRC_CI_AS
        processed_line = re.sub(r'COLLATE\s+Chinese_PRC_CI_AS', '', processed_line, flags=re.IGNORECASE)
        
        # 保留非空的处理后行
        if processed_line.strip() or not line.strip():
            processed_lines.append(processed_line if processed_line.strip() == '' else processed_line)
    
    # 重建SQL内容
    sql_content = '\n'.join(processed_lines)
    
    # 5. 更精确地处理CREATE TABLE语句
    # 使用正则表达式匹配CREATE TABLE语句 - 修复为非贪婪匹配
    create_table_pattern = re.compile(r'CREATE TABLE\s+\[?dbo\]?\.?\[?(\w+)\]?\s*\((.*?)\)\s*$', re.DOTALL | re.IGNORECASE | re.MULTILINE)
    
    def process_create_table(match):
        table_name = match.group(1)
        table_body = match.group(2)
        print(f"处理CREATE TABLE语句: {table_name}")
        print(f"表体内容前100字符: {repr(table_body[:100])}")
        
        # 处理列定义
        lines = table_body.split('\n')
        print(f"分割后的行数: {len(lines)}")
        processed_lines = []
        primary_key_col = None
        
        for i, line in enumerate(lines):
            original_line = line
            line = line.strip()
            if not line:
                continue
            
            print(f"处理第{i}行: {repr(original_line)}")
            
            # 处理IDENTITY列（在处理方括号之前）
            if 'IDENTITY(' in line:
                print(f"发现IDENTITY列: {line}")
                print(f"原始行内容: {repr(line)}")
                # 移除IDENTITY关键字
                line = re.sub(r'\s+IDENTITY\([^)]+\)', '', line)
                print(f"移除IDENTITY后: {repr(line)}")
                # 添加AUTO_INCREMENT
                line = re.sub(r'(\[?\w+\]?)\s+int\s+(NOT\s+NULL|NULL)', r'\1 int AUTO_INCREMENT \2', line)
                print(f"添加AUTO_INCREMENT后: {repr(line)}")
                # 记录主键列名
                pk_match = re.search(r'(\[?\w+\]?)\s+int\s+AUTO_INCREMENT\s+NOT\s+NULL', line)
                if pk_match:
                    primary_key_col = pk_match.group(1)
                    print(f"主键列: {primary_key_col}")
            
            # 移除方括号
            line = re.sub(r'\[(\w+)\]', r'\1', line)
            # 移除COLLATE子句
            line = re.sub(r'\s+COLLATE\s+[^\s,]+', '', line)
            # 转换数据类型
            line = re.sub(r'\bmoney\b', 'decimal(19,4)', line)
            
            processed_lines.append(line)
        
        # 重建表定义
        new_table_body = '\n  '.join(processed_lines)
        
        # 添加PRIMARY KEY约束（如果有AUTO_INCREMENT列）
        if primary_key_col and 'PRIMARY KEY' not in new_table_body:
            # 移除主键列名中的方括号
            clean_pk_col = primary_key_col.strip('[]')
            # 确保最后一行以逗号结尾
            if not new_table_body.strip().endswith(','):
                new_table_body += ','
            new_table_body += f'\n  PRIMARY KEY ({clean_pk_col})'
        
        # 重建CREATE TABLE语句
        return f'CREATE TABLE {table_name} ({new_table_body})'
    
    # 替换所有CREATE TABLE语句
    sql_content = create_table_pattern.sub(process_create_table, sql_content)
    
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
