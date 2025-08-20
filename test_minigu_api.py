#!/usr/bin/env python3
"""
Test script for miniGU Python API functionality
"""

print("Testing miniGU Python API...")

try:
    from minigu.python.minigu import MiniGU, AsyncMiniGU, connect, async_connect
    print("✓ Successfully imported miniGU Python API")
    
    # Test synchronous connection
    print("\n--- Testing synchronous API ---")
    try:
        db = connect()
        print("✓ Successfully created synchronous connection")
        print(f"Connection status: {db.is_connected}")
        print(f"Has Rust bindings: {db._rust_instance is not None}")
        
        # Test simple query
        result = db.execute("RETURN 'Hello, miniGU' AS greeting")
        print(f"✓ Simple query executed. Result: {result.data}")
        
        # Test data loading
        sample_data = [
            {"name": "Alice", "age": 30, "label": "Person"},
            {"name": "Bob", "age": 25, "label": "Person"}
        ]
        db.load(sample_data)
        print("✓ Sample data loaded")
        
        # Check if we can save
        try:
            db.save("test_output.mgu")
            print("✓ Database saved to test_output.mgu")
        except Exception as e:
            print(f"⚠ Save failed: {e}")
        
        db.close()
        print("✓ Connection closed")
        
    except Exception as e:
        print(f"✗ Synchronous API test failed: {e}")
    
    # Test asynchronous connection
    print("\n--- Testing asynchronous API ---")
    try:
        import asyncio
        
        async def test_async():
            db = await async_connect()
            print("✓ Successfully created asynchronous connection")
            print(f"Connection status: {db.is_connected}")
            print(f"Has Rust bindings: {db._rust_instance is not None}")
            
            # Test simple query
            result = await db.execute("RETURN 'Hello, miniGU' AS greeting")
            print(f"✓ Simple async query executed. Result: {result.data}")
            
            # Test data loading
            sample_data = [
                {"name": "Charlie", "age": 35, "label": "Person"}
            ]
            await db.load(sample_data)
            print("✓ Sample data loaded asynchronously")
            
            await db.close()
            print("✓ Async connection closed")
            
        asyncio.run(test_async())
        
    except Exception as e:
        print(f"✗ Asynchronous API test failed: {e}")

except ImportError as e:
    print(f"✗ Failed to import miniGU Python API: {e}")

print("\nAPI test completed.")