#!/usr/bin/env python3
"""
MySQL SQL执行器
读取SQL文件并连接MySQL数据库执行其中的SQL语句
"""

import argparse
import logging
import os
import re
import pymysql
from pymysql import Error
import time
from dotenv import load_dotenv

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('mysql_executor.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 加载.env文件
load_dotenv()


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
        logger.warning(f"文件扩展名不是标准的 .sql，但继续处理...")


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
        logger.info(f"成功读取文件: {sql_file}")
        logger.info(f"文件大小: {os.path.getsize(sql_file)} 字节")
        
        # 分割SQL语句
        statements = split_sql_statements(sql_content)
        logger.info(f"解析出 {len(statements)} 条SQL语句")
        
        if dry_run:
            logger.info("\n=== 干运行模式 - 以下是将要执行的SQL语句 ===")
            for i, statement in enumerate(statements, 1):
                logger.info(f"\n--- 语句 {i} ---")
                if len(statement) > 200:
                    logger.info(statement[:200] + "...")
                else:
                    logger.info(statement)
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
            logger.info(f"\n成功连接到MySQL数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")
            
            # 执行每条SQL语句
            for i, statement in enumerate(statements, 1):
                try:
                    # 跳过空语句和纯注释语句
                    if not statement.strip() or statement.strip().startswith('--'):
                        continue
                    
                    logger.info(f"执行语句 {i}/{len(statements)}: {statement[:50]}...")
                    cursor.execute(statement)
                    connection.commit()
                    success_count += 1
                    logger.info(f"✓ 执行成功")
                    
                except Error as e:
                    error_count += 1
                    error_message = str(e)
                    logger.error(f"✗ 执行失败: {error_message}")
                    
                    # 记录错误详情
                    error_log_path = 'sql_execution_errors.log'
                    error_detail = f"""
时间: {time.strftime('%Y-%m-%d %H:%M:%S')}
文件: {sql_file}
语句: {statement[:100]}...
错误: {error_message}
"""
                    
                    with open(error_log_path, 'a', encoding='utf-8') as error_log:
                        error_log.write(error_detail)
                    
                    logger.error(f"错误详情已记录到: {error_log_path}")
                    
                    # 回滚当前事务
                    connection.rollback()
                    logger.warning(f"事务已回滚")
                    
                    # 询问是否继续
                    if error_count >= 5:
                        choice = input(f"\n已发生 {error_count} 个错误，是否继续执行? (y/n): ")
                        if choice.lower() != 'y':
                            break
            
            logger.info(f"\n=== 执行结果 ===")
            logger.info(f"成功: {success_count} 条")
            logger.info(f"失败: {error_count} 条")
            logger.info(f"总计: {len(statements)} 条")
            
            return error_count == 0
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                logger.info("数据库连接已关闭")
                
    except Exception as e:
        logger.error(f"错误: {e}")
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
            logger.error("错误: 数据库名未指定，请通过--database参数或在.env文件中设置DB_DATABASE")
            return 1
        
        logger.info(f"数据库配置:")
        logger.info(f"  主机: {db_config['host']}:{db_config['port']}")
        logger.info(f"  用户: {db_config['user']}")
        logger.info(f"  数据库: {db_config['database']}")
        logger.info(f"  字符集: {db_config['charset']}")
        logger.info(f"  配置来源: {'命令行参数' if args.database else '.env文件'}")
        
        # 执行SQL文件
        success = execute_sql_file(sql_file, db_config, args.dry_run)
        
        if success:
            logger.info("\n✅ SQL文件执行完成")
            return 0
        else:
            logger.error("\n❌ SQL文件执行过程中出现错误")
            return 1
            
    except Exception as e:
        logger.error(f"错误: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
