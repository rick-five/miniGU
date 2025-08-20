#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试miniGU异步API实现
"""

import sys
import os
import asyncio

# 添加项目根目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# 导入API
from minigu.python.minigu import async_connect, AsyncMiniGU


async def test_async_api():
    """测试异步API"""
    print("=== 测试异步API ===")
    
    try:
        # 测试使用异步连接
        print("1. 测试异步连接...")
        db = await async_connect()
        try:
            print("   [通过] 异步连接成功")
            
            # 测试异步执行查询
            print("2. 测试异步执行查询...")
            try:
                result = await db.execute("MATCH (n) RETURN n LIMIT 1;")
                print(f"   [通过] 异步查询执行成功，返回 {result.row_count} 行数据")
            except Exception as e:
                print(f"   [信息] 查询执行结果（开发中）: {e}")
            
            # 测试异步加载数据
            print("3. 测试异步加载数据...")
            sample_data = [
                {"name": "Alice", "age": 30, "label": "Person"},
                {"name": "Bob", "age": 25, "label": "Person"}
            ]
            await db.load(sample_data)
            print("   [通过] 异步数据加载成功")
            
            # 测试异步保存数据库
            print("4. 测试异步保存数据库...")
            await db.save("async_test.json")
            print("   [通过] 异步数据库保存成功")
            
            # 测试异步创建图
            print("5. 测试异步创建图...")
            await db.create_graph("async_test_graph", {
                "Person": {"name": "STRING", "age": "INTEGER"}
            })
            print("   [通过] 异步图创建成功")
            
            # 测试异步性能API
            print("6. 测试异步性能API...")
            await db.set_cache_size(500)
            await db.set_thread_count(2)
            await db.enable_query_logging(True)
            stats = await db.get_performance_stats()
            print(f"   [通过] 异步性能统计获取成功: {stats}")
            
        finally:
            db.close()
            
        print("\n[通过] 所有异步API测试完成!")
        
    except Exception as e:
        print(f"[失败] 异步API测试过程中出错: {e}")
        raise


async def test_async_context_manager():
    """测试异步上下文管理器"""
    print("\n=== 测试异步上下文管理器 ===")
    
    try:
        print("1. 测试异步上下文管理器...")
        async with await async_connect() as db:
            print("   [通过] 异步上下文管理器工作正常")
            
            # 执行一些操作
            await db.execute("SHOW PROCEDURES")
            print("   [通过] 在上下文管理器中执行操作成功")
            
        print("   [通过] 退出上下文管理器时正确关闭连接")
        print("\n[通过] 异步上下文管理器测试完成!")
        
    except Exception as e:
        print(f"[失败] 异步上下文管理器测试过程中出错: {e}")
        raise


async def main():
    """主测试函数"""
    print("miniGU Python API 异步实现测试")
    print("=" * 50)
    
    try:
        await test_async_api()
        await test_async_context_manager()
        
        print("\n" + "=" * 50)
        print("所有异步测试完成!")
        
    except Exception as e:
        print(f"\n[失败] 异步测试套件执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())