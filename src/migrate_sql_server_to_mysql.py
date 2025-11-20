#!/usr/bin/env python3
"""
完整的数据迁移脚本
将SQL Server文件转换为MySQL格式并执行
"""

import argparse
import os
import subprocess
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='SQL Server到MySQL完整迁移脚本')
    parser.add_argument('sql_file', help='SQL Server SQL文件路径')
    parser.add_argument('--host', help='MySQL服务器地址 (默认从.env文件读取)')
    parser.add_argument('--port', type=int, help='MySQL服务器端口 (默认从.env文件读取)')
    parser.add_argument('--user', help='MySQL用户名 (默认从.env文件读取)')
    parser.add_argument('--password', help='MySQL密码 (默认从.env文件读取)')
    parser.add_argument('--database', help='要连接的数据库名 (默认从.env文件读取)')
    parser.add_argument('--charset', help='字符集 (默认从.env文件读取)')
    parser.add_argument('--convert-only', action='store_true', help='只转换不执行')
    parser.add_argument('--execute-only', action='store_true', help='只执行已转换的文件')
    return parser.parse_args()


def run_command(cmd, description):
    """运行命令并处理结果"""
    print(f"\n=== {description} ===")
    print(f"执行命令: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("警告:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"错误: {e}")
        print("标准输出:", e.stdout)
        print("错误输出:", e.stderr)
        return False


def main():
    """主函数"""
    args = parse_arguments()
    sql_file = args.sql_file
    
    try:
        # 验证输入文件
        if not os.path.exists(sql_file):
            print(f"错误: 文件 '{sql_file}' 不存在")
            return 1
        
        # 确定输出文件路径
        base_name = os.path.basename(sql_file)
        name_without_ext = os.path.splitext(base_name)[0]
        ext = os.path.splitext(base_name)[1]
        output_file = os.path.join("target-data", f"{name_without_ext}_mysql{ext}")
        
        # 获取数据库配置 - 从命令行参数或环境变量获取
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
            print("错误: 数据库名未指定，请通过--database参数或在.env文件中设置DB_DATABASE")
            return 1
        
        print(f"数据库配置:")
        print(f"  主机: {db_config['host']}:{db_config['port']}")
        print(f"  用户: {db_config['user']}")
        print(f"  数据库: {db_config['database']}")
        print(f"  字符集: {db_config['charset']}")
        print(f"  配置来源: {'命令行参数' if args.database else '.env文件'}")
        
        # 转换步骤
        if not args.execute_only:
            convert_cmd = [
                "python3", "src/sql_server_to_mysql.py", sql_file
            ]
            if not run_command(convert_cmd, "转换SQL Server到MySQL"):
                return 1
        
        # 执行步骤
        if not args.convert_only:
            execute_cmd = [
                "python3", "src/mysql_sql_executor.py", output_file,
                "--host", db_config['host'],
                "--port", str(db_config['port']),
                "--user", db_config['user'],
                "--password", db_config['password'],
                "--database", db_config['database'],
                "--charset", db_config['charset']
            ]
            if not run_command(execute_cmd, "执行MySQL SQL"):
                return 1
        
        print("\n✅ 数据迁移完成")
        return 0
        
    except Exception as e:
        print(f"错误: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
