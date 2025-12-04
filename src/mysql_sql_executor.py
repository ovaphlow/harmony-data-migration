#!/usr/bin/env python3
"""
MySQL SQL执行器
读取SQL文件并连接MySQL数据库执行其中的SQL语句
修改：实现错误不中断运行，错误输出到日志，执行完毕显示统计信息
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
    """自定义JSON格式化器 - 用于文件输出"""

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


class ColoredFormatter(logging.Formatter):
    """自定义彩色格式化器 - 用于控制台输出"""

    # 颜色定义
    COLORS = {
        'DEBUG': '\033[36m',     # 青色
        'INFO': '\033[32m',      # 绿色
        'WARNING': '\033[33m',   # 黄色
        'ERROR': '\033[31m',     # 红色
        'CRITICAL': '\033[35m',  # 紫色
        'RESET': '\033[0m'       # 重置
    }

    def format(self, record):
        # 获取时间戳
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')

        # 格式化级别名称
        level_name = record.levelname
        color = self.COLORS.get(level_name, self.COLORS['RESET'])
        reset = self.COLORS['RESET']

        # 构建基本消息
        message = f"{color}[{timestamp}] {level_name:<8}{reset} {record.getMessage()}"

        # 添加额外的上下文信息
        extra_info = []
        if hasattr(record, 'sql_file'):
            extra_info.append(f"SQL文件: {record.sql_file}")
        if hasattr(record, 'statement_count'):
            extra_info.append(f"语句数量: {record.statement_count}")
        if hasattr(record, 'statement_index'):
            extra_info.append(f"语句索引: {record.statement_index}/{getattr(record, 'statement_count', '')}")
        if hasattr(record, 'statement_preview'):
            extra_info.append(f"语句预览: {record.statement_preview}")
        if hasattr(record, 'error_message'):
            extra_info.append(f"错误信息: {record.error_message}")
        if hasattr(record, 'success_count'):
            extra_info.append(f"成功数量: {record.success_count}")
        if hasattr(record, 'error_count'):
            extra_info.append(f"错误数量: {record.error_count}")
        if hasattr(record, 'database_host'):
            extra_info.append(f"数据库: {record.database_host}:{getattr(record, 'database_port', '')}/{record.database_name}")
        if hasattr(record, 'execution_time'):
            extra_info.append(f"执行时间: {record.execution_time:.3f}s")
        if hasattr(record, 'file_size'):
            extra_info.append(f"文件大小: {record.file_size} bytes")
        if hasattr(record, 'error_log_path'):
            extra_info.append(f"错误日志: {record.error_log_path}")
        if hasattr(record, 'config_source'):
            extra_info.append(f"配置来源: {record.config_source}")
        if hasattr(record, 'dry_run'):
            extra_info.append(f"干运行: {record.dry_run}")

        # 添加模块信息
        if record.module:
            extra_info.append(f"模块: {record.module}.{record.funcName}:{record.lineno}")

        if extra_info:
            message += f" | {' | '.join(extra_info)}"

        return message


class WarningErrorFilter(logging.Filter):
    """过滤器 - 只允许WARNING和ERROR级别的日志"""

    def filter(self, record):
        return record.levelno >= logging.WARNING


# 加载.env文件
load_dotenv()

# 配置日志记录
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建格式化器
json_formatter = JSONFormatter()
colored_formatter = ColoredFormatter()
warning_error_filter = WarningErrorFilter()

# 控制台输出 - 使用彩色格式输出所有级别日志
console_handler = logging.StreamHandler()
console_handler.setFormatter(colored_formatter)
console_handler.setLevel(logging.INFO)  # 控制台显示INFO及以上级别
logger.addHandler(console_handler)

# 文件输出 - 保存为JSONL格式，只记录WARNING和ERROR级别
file_handler = logging.FileHandler('mysql_executor_warnings_errors.jsonl', encoding='utf-8')
file_handler.setFormatter(json_formatter)
file_handler.setLevel(logging.WARNING)  # 文件只记录WARNING及以上级别
file_handler.addFilter(warning_error_filter)  # 添加过滤器确保只记录warning和error
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


def print_execution_summary(total_statements, success_count, error_count, sql_file):
    """打印执行完毕时的统计信息"""
    print("\n" + "="*60)
    print("SQL文件执行完毕 - 统计报告")
    print("="*60)
    print(f"SQL文件路径: {sql_file}")
    print(f"总执行语句数量: {total_statements}")
    print(f"成功执行语句数量: {success_count}")
    print(f"失败执行语句数量: {error_count}")
    print(f"执行成功率: {(success_count/total_statements*100):.2f}%" if total_statements > 0 else "执行成功率: N/A")

    if error_count > 0:
        print(f"\n⚠️  注意: 有 {error_count} 个SQL语句执行失败")
        print("错误详情已记录到日志文件: sql_execution_errors.jsonl")
    else:
        print(f"\n✅ 所有SQL语句执行成功!")

    print("="*60 + "\n")


def execute_sql_file(sql_file, db_config, dry_run=False):
    """
    读取SQL文件并执行其中的SQL语句
    修改：实现错误不中断运行，持续执行所有语句
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
        total_statements = len([s for s in statements if s.strip() and not s.strip().startswith('--')])
        extra_info = {'sql_file': sql_file, 'statement_count': total_statements}
        logger.info("解析SQL语句完成", extra=extra_info)

        if dry_run:
            extra_info = {'sql_file': sql_file, 'statement_count': total_statements}
            logger.info("开始干运行模式", extra=extra_info)
            for i, statement in enumerate(statements, 1):
                if statement.strip() and not statement.strip().startswith('--'):
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
            valid_statements = [s for s in statements if s.strip() and not s.strip().startswith('--')]
            for i, statement in enumerate(valid_statements, 1):
                try:
                    statement_preview = statement[:50] + "..." if len(statement) > 50 else statement
                    extra_info = {
                        'sql_file': sql_file,
                        'statement_index': i,
                        'statement_count': total_statements,
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

                    # 继续执行，不询问是否继续

            # 打印最终统计信息
            print_execution_summary(total_statements, success_count, error_count, sql_file)

            return True  # 即使有错误也返回True，表示执行完成

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

        return 0  # 始终返回0，因为错误不中断执行

    except Exception as e:
        extra_info = {'sql_file': sql_file, 'error_message': str(e)}
        logger.error("主函数执行时发生错误", extra=extra_info)
        return 1


if __name__ == "__main__":
    exit(main())
