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


# 加载.env文件
load_dotenv()

# 配置日志记录
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建格式化器
json_formatter = JSONFormatter()
colored_formatter = ColoredFormatter()

# 控制台输出 - 使用彩色格式输出所有级别日志
console_handler = logging.StreamHandler()
console_handler.setFormatter(colored_formatter)
console_handler.setLevel(logging.INFO)  # 控制台显示INFO及以上级别
logger.addHandler(console_handler)


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


def preprocess_sql_content(sql_content):
    """
    预处理SQL内容，处理特殊字符和编码问题
    修复：改进字符串中换行符的处理，避免错误分割
    """
    # 处理HTML实体编码
    html_entities = {
        '&nbsp;': ' ',
        '&lt;': '<',
        '&gt;': '>',
        '&amp;': '&',
        '&quot;': '"',
        '&apos;': "'",
        '&micro;': 'μ',
        '&deg;': '°',
        '&alpha;': 'α',
        '&beta;': 'β',
        '&gamma;': 'γ',
        '&delta;': 'δ',
        '&epsilon;': 'ε',
        '&theta;': 'θ',
        '&lambda;': 'λ',
        '&mu;': 'μ',
        '&pi;': 'π',
        '&sigma;': 'σ',
        '&phi;': 'φ',
        '&omega;': 'ω'
    }

    # 替换HTML实体编码
    for entity, char in html_entities.items():
        sql_content = sql_content.replace(entity, char)

    # 处理可能的编码问题，如乱码字符
    encoding_fixes = {
        'Ã§': 'ç',
        'Ã¥': 'å',
        'Ã¨': 'è',
        'Ã©': 'é',
        'Ãª': 'ê',
        'Ã«': 'ë',
        'Ã¬': 'ì',
        'Ã­': 'í',
        'Ã®': 'î',
        'Ã¯': 'ï',
        'Ã±': 'ñ',
        'Ã²': 'ò',
        'Ã³': 'ó',
        'Ã´': 'ô',
        'Ãµ': 'õ',
        'Ã¶': 'ö',
        'Ã¹': 'ù',
        'Ãº': 'ú',
        'Ã»': 'û',
        'Ã¼': 'ü',
        'Ã¿': 'ÿ'
    }

    # 替换编码问题
    for wrong, correct in encoding_fixes.items():
        sql_content = sql_content.replace(wrong, correct)

    # 不再处理字符串中的换行符，让split_sql_statements函数直接处理
    # 这样可以避免错误地转义换行符导致的问题
    return sql_content


def split_sql_statements(sql_content):
    """
    将SQL内容按分号分割成独立的SQL语句
    处理特殊情况：字符串中的分号不应作为分隔符
    增强处理：支持括号嵌套和更复杂的SQL语句结构
    改进：正确处理INSERT语句的分割，避免多个INSERT语句被合并
    修复：解决缺少VALUES子句的INSERT语句、未匹配单引号和非SQL文本问题
    """
    # 预处理SQL内容
    sql_content = preprocess_sql_content(sql_content)

    # 移除注释
    sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)
    sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)

    # 按分号分割，但要考虑字符串中的分号和括号嵌套
    statements = []
    current_statement = ""
    in_string = False
    string_char = None
    escape_next = False
    paren_count = 0  # 括号计数器，用于处理嵌套结构
    bracket_count = 0  # 方括号计数器
    brace_count = 0  # 花括号计数器

    i = 0
    while i < len(sql_content):
        char = sql_content[i]

        # 处理转义字符 - 先处理转义，再处理其他逻辑
        if char == '\\' and in_string:
            escape_next = True
            current_statement += char
            i += 1
            continue

        # 处理字符串 - 改进版本，更好地处理转义和嵌套
        if char in ("'", '"') and not escape_next:
            if not in_string:
                # 开始一个新字符串
                in_string = True
                string_char = char
                current_statement += char
                i += 1
                continue
            elif char == string_char:
                # 检查前面的字符数量，确定这个引号是否被转义
                backslash_count = 0
                j = i - 1
                while j >= 0 and sql_content[j] == '\\':
                    backslash_count += 1
                    j -= 1

                # 如果反斜杠数量是偶数，则这个引号没有被转义
                if backslash_count % 2 == 0:
                    # 检查MySQL的连续引号转义方式
                    if i + 1 < len(sql_content) and sql_content[i + 1] == char:
                        # 连续两个相同的引号，这是MySQL中的转义方式
                        current_statement += char + char
                        i += 2
                        continue
                    else:
                        # 字符串结束
                        in_string = False
                        string_char = None
                        current_statement += char
                        i += 1
                        continue
                else:
                    # 引号被反斜杠转义，作为字符串内容处理
                    current_statement += char
                    i += 1
                    continue
            else:
                # 不同类型的引号，作为字符串内容处理
                current_statement += char
                i += 1
                continue

        # 处理括号嵌套（只在不在字符串中时计数）
        if not in_string:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count = max(0, paren_count - 1)
            elif char == '[':
                bracket_count += 1
            elif char == ']':
                bracket_count = max(0, bracket_count - 1)
            elif char == '{':
                brace_count += 1
            elif char == '}':
                brace_count = max(0, brace_count - 1)

        # 处理分号（只在不在字符串中且所有括号都平衡时才分割）
        if char == ';' and not in_string and paren_count == 0 and bracket_count == 0 and brace_count == 0:
            # 语句结束
            statement = current_statement.strip()
            if statement:  # 只添加非空语句
                statements.append(statement)
            current_statement = ""
            i += 1
            continue
        else:
            current_statement += char

        i += 1

    # 添加最后一个语句（如果没有以分号结尾）
    last_statement = current_statement.strip()
    if last_statement and last_statement != ';':
        statements.append(last_statement)

    # 进一步处理可能包含多个INSERT语句的情况
    # 检查每个语句，如果包含多个INSERT INTO，尝试进一步分割
    final_statements = []
    for stmt in statements:
        # 检查是否包含多个INSERT INTO
        insert_count = stmt.upper().count("INSERT INTO")
        if insert_count > 1:
            # 尝试在INSERT INTO之间分割
            # 使用正则表达式找到所有INSERT INTO的位置
            insert_positions = []
            for match in re.finditer(r'INSERT\s+INTO', stmt, re.IGNORECASE):
                insert_positions.append(match.start())

            # 按INSERT INTO位置分割语句
            for i in range(len(insert_positions)):
                start_pos = insert_positions[i]
                end_pos = insert_positions[i + 1] if i + 1 < len(insert_positions) else len(stmt)

                # 提取子语句
                sub_stmt = stmt[start_pos:end_pos].strip()

                # 确保子语句以分号结尾（所有子语句，包括最后一个）
                # 查找最后一个VALUES子句的结束位置
                values_match = re.search(r'VALUES\s*\([^)]*\)\s*;?\s*$', sub_stmt, re.IGNORECASE | re.DOTALL)
                if values_match:
                    # 确保有分号
                    if not sub_stmt.endswith(';'):
                        sub_stmt += ';'
                else:
                    # 如果找不到完整的VALUES子句，可能需要进一步处理
                    # 尝试找到最后一个右括号
                    last_paren = sub_stmt.rfind(')')
                    if last_paren > 0:
                        # 在右括号后添加分号
                        sub_stmt = sub_stmt[:last_paren+1] + ';'
                    else:
                        # 如果找不到右括号，直接添加分号
                        sub_stmt = sub_stmt.rstrip() + ';'

                final_statements.append(sub_stmt)
        else:
            # 对于单个INSERT语句，检查是否以分号结尾
            # 如果是INSERT语句但不以分号结尾，添加分号
            if stmt.upper().startswith("INSERT") and not stmt.rstrip().endswith(';'):
                # 检查是否包含VALUES子句
                if "VALUES" in stmt.upper():
                    # 尝试找到最后一个右括号
                    last_paren = stmt.rfind(')')
                    if last_paren > 0:
                        # 在右括号后添加分号
                        stmt = stmt[:last_paren+1] + ';'
                    else:
                        # 如果找不到右括号，直接添加分号
                        stmt = stmt.rstrip() + ';'

            final_statements.append(stmt)

    # 过滤掉明显不是SQL语句的片段
    filtered_statements = []
    for stmt in final_statements:
        # 清理语句前后的空白字符
        stmt = stmt.strip()

        # 跳过空语句
        if not stmt:
            continue

        # 对于INSERT语句，检查是否包含VALUES子句
        if stmt.upper().startswith("INSERT") and "VALUES" not in stmt.upper():
            # 如果是INSERT语句但不包含VALUES子句，跳过
            continue

        # 检查单引号是否匹配
        # 计算单引号数量，忽略转义的单引号
        quote_count = 0
        escape_next = False
        for char in stmt:
            if escape_next:
                escape_next = False
                continue
            if char == '\\':
                escape_next = True
                continue
            if char == "'":
                quote_count += 1

        # 如果单引号数量是奇数，说明有未匹配的单引号
        if quote_count % 2 != 0:
            # 尝试修复未匹配的单引号
            # 查找最后一个单引号的位置
            last_quote_pos = stmt.rfind("'")
            if last_quote_pos > 0:
                # 在最后一个单引号后添加一个单引号
                stmt = stmt[:last_quote_pos+1] + "'" + stmt[last_quote_pos+1:]

        filtered_statements.append(stmt)

    return filtered_statements


def execute_sql_file(sql_file, db_config, dry_run=False):
    """
    读取SQL文件并执行其中的SQL语句
    修改：实现错误不中断运行，持续执行所有语句
    增强错误处理和调试信息
    """
    try:
        # 在程序启动时删除之前的日志文件
        error_log_path = 'sql_execution_errors.jsonl'
        if os.path.exists(error_log_path):
            os.remove(error_log_path)
            logger.info(f"已删除之前的日志文件: {error_log_path}")

        # 使用UTF-8编码读取文件，处理中文和特殊字符
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        file_size = os.path.getsize(sql_file)
        extra_info = {'sql_file': sql_file, 'file_size': file_size}
        logger.info("成功读取SQL文件", extra=extra_info)
        logger.info(f"文件内容大小: {len(sql_content)} 字符", extra=extra_info)

        # 分割SQL语句
        statements = split_sql_statements(sql_content)
        # 不再进行额外过滤，因为split_sql_statements已经处理了所有必要的过滤和修正
        valid_statements = statements

        total_statements = len(valid_statements)
        extra_info = {'sql_file': sql_file, 'statement_count': total_statements}
        logger.info("解析SQL语句完成", extra=extra_info)

        if dry_run:
            extra_info = {'sql_file': sql_file, 'statement_count': total_statements}
            logger.info("开始干运行模式", extra=extra_info)
            for i, statement in enumerate(valid_statements, 1):
                if statement.strip() and not statement.strip().startswith('--'):
                    statement_preview = statement[:200] + "..." if len(statement) > 200 else statement
                    # 检查语句中是否包含特殊字符
                    has_special_chars = any(ord(c) > 127 for c in statement)
                    extra = {
                        'sql_file': sql_file,
                        'statement_index': i,
                        'statement_preview': statement_preview,
                        'statement_length': len(statement),
                        'has_special_chars': has_special_chars
                    }
                    logger.info("SQL语句预览", extra=extra)

            # 在干运行模式下也打印统计信息
            print_execution_summary(total_statements, total_statements, 0, sql_file)
            return True

        # 连接数据库并执行
        connection = None
        cursor = None
        success_count = 0
        error_count = 0

        try:
            # 设置连接参数，确保字符集正确
            connection = pymysql.connect(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database'],
                charset=db_config['charset'],
                use_unicode=True,     # 确保返回Unicode字符串
                autocommit=False      # 禁用自动提交，使用事务
            )
            cursor = connection.cursor()

            # 设置会话字符集以确保正确处理特殊字符
            cursor.execute("SET NAMES utf8mb4")
            cursor.execute("SET CHARACTER SET utf8mb4")
            cursor.execute("SET collation_connection = 'utf8mb4_unicode_ci'")

            extra_info = {
                'sql_file': sql_file,
                'database_host': db_config['host'],
                'database_port': db_config['port'],
                'database_name': db_config['database'],
                'database_charset': db_config['charset']
            }
            logger.info("成功连接到MySQL数据库", extra=extra_info)

            # 执行每条SQL语句
            for i, statement in enumerate(valid_statements, 1):
                try:
                    statement_preview = statement[:50] + "..." if len(statement) > 50 else statement
                    statement_length = len(statement)
                    has_special_chars = any(ord(c) > 127 for c in statement)

                    # 将INSERT语句改为INSERT IGNORE以避免主键冲突
                    if statement.strip().upper().startswith('INSERT INTO'):
                        statement = statement.replace('INSERT INTO', 'INSERT IGNORE INTO', 1)

                    extra_info = {
                        'sql_file': sql_file,
                        'statement_index': i,
                        'statement_count': total_statements,
                        'statement_preview': statement_preview,
                        'statement_length': statement_length,
                        'has_special_chars': has_special_chars
                    }
                    logger.info("开始执行SQL语句", extra=extra_info)

                    # 对于特别长的语句，记录额外调试信息
                    if statement_length > 10000:
                        logger.debug(f"执行长语句 {i}，长度: {statement_length}", extra=extra_info)
                        logger.debug(f"语句开头: {statement[:100]}...", extra=extra_info)
                        logger.debug(f"语句结尾: ...{statement[-100:]}", extra=extra_info)

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
                    error_code = getattr(e, 'args', [None])[0] if e.args else None
                    statement_preview = statement[:100] + "..." if len(statement) > 100 else statement

                    # 创建详细的错误信息
                    error_detail = {
                        'timestamp': datetime.now().isoformat(),
                        'sql_file': sql_file,
                        'statement_index': i,
                        'statement_preview': statement_preview,
                        'error_message': error_message,
                        'error_code': error_code,
                        'error_type': type(e).__name__,
                        'statement_length': len(statement),
                        'has_special_chars': any(ord(c) > 127 for c in statement),
                        'full_statement': statement
                    }

                    # 如果是语法错误，添加更多调试信息
                    if error_code == 1064 or "syntax error" in error_message.lower():
                        if len(statement) > 200:
                            error_detail['statement_start'] = statement[:100]
                            error_detail['statement_end'] = statement[-100:]

                        # 检查常见的语法问题
                        if "'" in statement and statement.count("'") % 2 != 0:
                            error_detail['possible_issue'] = "未匹配的单引号"
                        if '"' in statement and statement.count('"') % 2 != 0:
                            error_detail['possible_issue'] = "未匹配的双引号"

                    extra_info = {
                        'sql_file': sql_file,
                        'statement_index': i,
                        'statement_preview': statement_preview,
                        'error_message': error_message,
                        'error_code': error_code,
                        'success_count': success_count,
                        'error_count': error_count
                    }
                    logger.error("SQL语句执行失败", extra=extra_info)

                    # 记录错误详情到单独的JSONL文件
                    error_log_path = 'sql_execution_errors.jsonl'
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
        extra_info = {
            'sql_file': sql_file,
            'error_message': str(e),
            'error_type': type(e).__name__,
            'error_traceback': str(e.__traceback__) if e.__traceback__ else None
        }
        logger.error("执行SQL文件时发生错误", extra=extra_info)
        return False


def print_execution_summary(total_statements, success_count, error_count, sql_file):
    """
    打印SQL执行统计信息

    参数:
        total_statements: 总语句数
        success_count: 成功执行的语句数
        error_count: 执行失败的语句数
        sql_file: SQL文件路径
    """
    success_rate = (success_count / total_statements * 100) if total_statements > 0 else 0

    print("\n" + "="*80)
    print("SQL执行统计报告")
    print("="*80)
    print(f"SQL文件: {sql_file}")
    print(f"总语句数: {total_statements}")
    print(f"成功执行: {success_count}")
    print(f"执行失败: {error_count}")
    print(f"成功率: {success_rate:.2f}%")
    print("="*80)

    # 记录到日志
    extra_info = {
        'sql_file': sql_file,
        'total_statements': total_statements,
        'success_count': success_count,
        'error_count': error_count,
        'success_rate': f"{success_rate:.2f}%"
    }
    logger.info("SQL执行统计报告", extra=extra_info)


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
