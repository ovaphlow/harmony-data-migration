#!/usr/bin/env python3
"""
MySQL SQL执行器
读取SQL文件并连接MySQL数据库执行其中的SQL语句
"""

import argparse
import json
import logging
import os
import re
import pymysql
from pymysql import Error
import time
from datetime import datetime
from dotenv import load_dotenv


class JSONFormatter(logging.Formatter):
    """自定义JSON格式化器"""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # 添加额外的上下文信息
        if hasattr(record, 'sql_file'):
            log_entry['sql_file'] = record.sql_file
        if hasattr(record, 'statement_count'):
            log_entry['statement_count'] = record.statement_count
        if hasattr(record, 'statement_index'):
            log_entry['statement_index'] = record.statement_index
        if hasattr(record, 'statement_preview'):
            log_entry['statement_preview'] = record.statement_preview
        if hasattr(record, 'error_message'):
            log_entry['error_message'] = record.error_message
        if hasattr(record, 'success_count'):
            log_entry['success_count'] = record.success_count
        if hasattr(record, 'error_count'):
            log_entry['error_count'] = record.error_count
        if hasattr(record, 'database_host'):
            log_entry['database_host'] = record.database_host
        if hasattr(record, 'database_port'):
            log_entry['database_port'] = record.database_port
        if hasattr(record, 'database_name'):
            log_entry['database_name'] = record.database_name
        if hasattr(record, 'execution_time'):
            log_entry['execution_time'] = record.execution_time
        if hasattr(record, 'file_size'):
            log_entry['file_size'] = record.file_size
        if hasattr(record, 'error_log_path'):
            log_entry['error_log_path'] = record.error_log_path
        if hasattr(record, 'config_source'):
            log_entry['config_source'] = record.config_source
        if hasattr(record, 'dry_run'):
            log_entry['dry_run'] = record.dry_run
            
        return json.dumps(log_entry, ensure_ascii=False)


# 加载.env文件
load_dotenv()

# 配置日志记录 - 使用JSON格式并保存为jsonl文件
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建JSON格式化器
json_formatter = JSONFormatter()

# 控制台输出 - 使用JSON格式但保持可读性
console_handler = logging.StreamHandler()
console_handler.setFormatter(json_formatter)
logger.addHandler(console_handler)

# 文件输出 - 保存为JSONL格式
file_handler = logging.FileHandler('mysql_executor.jsonl', encoding='utf-8')
file_handler.setFormatter(json_formatter)
logger.addHandler(file_handler)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='MySQL SQL执行器 - 读取SQL文件并执行SQL语句')
    parser.add_argument('sql_file', help='要执行的SQL文件路径')
    parser.add_argument('--host', help='MySQL服务器地址 (默认从.env文件读取)')
    parser.add_argument('--port', type=int, help='MySQL服务器端口 (默认从.env文件读取)')
    parser.add_argument('--user', help='MySQL用户名 (默认从.env文件读取)')
    parser.add_argument('--password', help='MySQL密码 (默认从.env文件读取)')
    parser.add_argument('--database', help='要连接的数据库名 (默认从.env文件读取)')
    parser.add_argument('--charset', help='字符集 (默认从.env文件读取)')
    parser.add_argument('--dry-run', action='store_true', help='只解析SQL语句，不实际执行')
    return parser.parse_args()


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
        extra = {'sql_file': file_path}
        logger.warning("文件扩展名不是标准的 .sql，但继续处理...", extra=extra)


def split_sql_statements(sql_content):
    """
    将SQL内容按分号分割成独立的SQL语句
    处理特殊情况：字符串中的分号不应作为分隔符
    """
    # 移除注释
    sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
    sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
    
    # 按分号分割，但要考虑字符串中的分号
    statements = []
    current_statement = ""
    in_string = False
    escape_next = False
    
    for char in sql_content:
        if escape_next:
            current_statement += char
            escape_next = False
            continue
            
        if char == '\\' and in_string:
            escape_next = True
            current_statement += char
            continue
            
        if char in ("'", '"') and not escape_next:
            in_string = not in_string
            current_statement += char
            continue
            
        if char == ';' and not in_string:
            # 语句结束
            statement = current_statement.strip()
            if statement:  # 只添加非空语句
                statements.append(statement)
            current_statement = ""
        else:
            current_statement += char
    
    # 添加最后一个语句（如果没有以分号结尾）
    last_statement = current_statement.strip()
    if last_statement:
        statements.append(last_statement)
    
    return statements


def execute_sql_file(sql_file, db_config, dry_run=False):
    """
    读取SQL文件并执行其中的SQL语句
    """
    try:
        # 读取文件内容
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        file_size = os.path.getsize(sql_file)
        extra_info = {'sql_file': sql_file, 'file_size': file_size}
        logger.info("成功读取SQL文件", extra=extra_info)
        
        # 分割SQL语句
        statements = split_sql_statements(sql_content)
        extra_info = {'sql_file': sql_file, 'statement_count': len(statements)}
        logger.info("解析SQL语句完成", extra=extra_info)
        
        if dry_run:
            extra_info = {'sql_file': sql_file, 'statement_count': len(statements)}
            logger.info("开始干运行模式", extra=extra_info)
            for i, statement in enumerate(statements, 1):
                statement_preview = statement[:200] + "..." if len(statement) > 200 else statement
                extra = {'sql_file': sql_file, 'statement_index': i, 'statement_preview': statement_preview}
                logger.info("SQL语句预览", extra=extra)
            return True
        
        # 连接数据库并执行
        connection = None
        cursor = None
        success_count = 0
        error_count = 0
        
        try:
            # 连接数据库
            connection = pymysql.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database'],
                charset=db_config['charset']
            )
            cursor = connection.cursor()
            extra_info = {
                'sql_file': sql_file,
                'database_host': db_config['host'],
                'database_port': db_config['port'],
                'database_name': db_config['database']
            }
            logger.info("成功连接到MySQL数据库", extra=extra_info)
            
            # 执行每条SQL语句
            for i, statement in enumerate(statements, 1):
                try:
                    # 跳过空语句和纯注释语句
                    if not statement.strip() or statement.strip().startswith('--'):
                        continue
                    
                    statement_preview = statement[:50] + "..." if len(statement) > 50 else statement
                    extra_info = {
                        'sql_file': sql_file,
                        'statement_index': i,
                        'statement_count': len(statements),
                        'statement_preview': statement_preview
                    }
                    logger.info("开始执行SQL语句", extra=extra_info)
                    
                    start_time = time.time()
                    cursor.execute(statement)
                    connection.commit()
                    execution_time = time.time() - start_time
                    success_count += 1
                    extra_info['execution_time'] = execution_time
                    logger.info("SQL语句执行成功", extra=extra_info)
                    
                except Error as e:
                    error_count += 1
                    error_message = str(e)
                    statement_preview = statement[:100] + "..." if len(statement) > 100 else statement
                    extra_info = {
                        'sql_file': sql_file,
                        'statement_index': i,
                        'statement_preview': statement_preview,
                        'error_message': error_message,
                        'success_count': success_count,
                        'error_count': error_count
                    }
                    logger.error("SQL语句执行失败", extra=extra_info)
                    
                    # 记录错误详情到单独的JSONL文件
                    error_log_path = 'sql_execution_errors.jsonl'
                    error_detail = {
                        'timestamp': datetime.now().isoformat(),
                        'sql_file': sql_file,
                        'statement_index': i,
                        'statement_preview': statement_preview,
                        'error_message': error_message,
                        'full_statement': statement
                    }
                    
                    with open(error_log_path, 'a', encoding='utf-8') as error_log:
                        error_log.write(json.dumps(error_detail, ensure_ascii=False) + '\n')
                    
                    extra_info_for_log = {'error_log_path': error_log_path}
                    logger.info("错误详情已记录", extra=extra_info_for_log)
                    
                    # 回滚当前事务
                    connection.rollback()
                    extra_info_rollback = {'sql_file': sql_file, 'statement_index': i}
                    logger.warning("事务已回滚", extra=extra_info_rollback)
                    
                    # 询问是否继续
                    if error_count >= 5:
                        choice = input(f"\n已发生 {error_count} 个错误，是否继续执行? (y/n): ")
                        if choice.lower() != 'y':
                            break
            
            final_stats = {
                'sql_file': sql_file,
                'success_count': success_count,
                'error_count': error_count,
                'total_statements': len(statements)
            }
            logger.info("SQL执行完成", extra=final_stats)
            
            return error_count == 0
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                extra_info = {
                    'sql_file': sql_file,
                    'database_host': db_config['host'],
                    'database_port': db_config['port'],
                    'database_name': db_config['database']
                }
                logger.info("数据库连接已关闭", extra=extra_info)
                
    except Exception as e:
        extra_info = {'sql_file': sql_file, 'error_message': str(e)}
        logger.error("执行SQL文件时发生错误", extra=extra_info)
        return False


def main():
    """主函数"""
    args = parse_arguments()
    sql_file = args.sql_file
    
    try:
        # 验证输入文件
        validate_input_file(sql_file)
        
        # 数据库配置 - 从命令行参数或环境变量获取
        db_config = {
            'host': args.host if args.host is not None else os.getenv('DB_HOST', 'localhost'),
            'port': args.port if args.port is not None else int(os.getenv('DB_PORT', '3306')),
            'user': args.user if args.user is not None else os.getenv('DB_USER', 'root'),
            'password': args.password if args.password is not None else os.getenv('DB_PASSWORD', ''),
            'database': args.database if args.database is not None else os.getenv('DB_DATABASE'),
            'charset': args.charset if args.charset is not None else os.getenv('DB_CHARSET', 'utf8mb4')
        }
        
        # 检查必需的数据库名
        if not db_config['database']:
            extra_info = {'sql_file': sql_file}
            logger.error("数据库名未指定", extra=extra_info)
            return 1
        
        db_config_info = {
            'sql_file': sql_file,
            'database_host': db_config['host'],
            'database_port': db_config['port'],
            'database_user': db_config['user'],
            'database_name': db_config['database'],
            'database_charset': db_config['charset'],
            'config_source': '命令行参数' if args.database else '.env文件'
        }
        logger.info("数据库配置已加载", extra=db_config_info)
        
        # 执行SQL文件
        success = execute_sql_file(sql_file, db_config, args.dry_run)
        
        final_result = {
            'sql_file': sql_file,
            'dry_run': args.dry_run
        }
        
        if success:
            logger.info("SQL文件执行完成", extra=final_result)
            return 0
        else:
            logger.error("SQL文件执行失败", extra=final_result)
            return 1
            
    except Exception as e:
        extra_info = {'sql_file': sql_file, 'error_message': str(e)}
        logger.error("主函数执行时发生错误", extra=extra_info)
        return 1


if __name__ == "__main__":
    exit(main())
