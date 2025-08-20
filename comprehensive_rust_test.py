#!/usr/bin/env python3
"""
Comprehensive test script to check Rust bindings functionality
"""

print("Comprehensive Rust bindings functionality test...")

try:
    from minigu.python import minigu_python
    from minigu.python.minigu_python import PyMiniGU
    
    print("✓ Successfully imported Rust bindings")
    
    # Create instance and initialize
    db = PyMiniGU()
    db.init()
    print("✓ Successfully initialized PyMiniGU")
    
    # Test execute method with a simple query
    try:
        result = db.execute("RETURN 1 AS number;")
        print(f"✓ Execute method works. Result: {result}")
    except Exception as e:
        print(f"⚠ Execute method failed: {e}")
    
    # Test methods availability
    methods_to_test = [
        'load_from_file',
        'save_to_file', 
        'create_graph',
        'insert_data',
        'update_data',
        'delete_data',
        'set_cache_size',
        'set_thread_count',
        'enable_query_logging',
        'get_performance_stats',
        'close'
    ]
    
    available_methods = []
    missing_methods = []
    
    for method in methods_to_test:
        if hasattr(db, method):
            available_methods.append(method)
        else:
            missing_methods.append(method)
    
    print(f"\nAvailable methods: {len(available_methods)}")
    for method in available_methods:
        print(f"  ✓ {method}")
        
    print(f"\nMissing methods: {len(missing_methods)}")
    for method in missing_methods:
        print(f"  ✗ {method}")
    
    # Test configuration methods
    try:
        if hasattr(db, 'set_cache_size'):
            db.set_cache_size(100)
            print("✓ set_cache_size works")
    except Exception as e:
        print(f"⚠ set_cache_size failed: {e}")
        
    try:
        if hasattr(db, 'set_thread_count'):
            db.set_thread_count(2)
            print("✓ set_thread_count works")
    except Exception as e:
        print(f"⚠ set_thread_count failed: {e}")
        
    try:
        if hasattr(db, 'enable_query_logging'):
            db.enable_query_logging(True)
            print("✓ enable_query_logging works")
    except Exception as e:
        print(f"⚠ enable_query_logging failed: {e}")
        
    try:
        if hasattr(db, 'get_performance_stats'):
            stats = db.get_performance_stats()
            print(f"✓ get_performance_stats works. Stats: {stats}")
    except Exception as e:
        print(f"⚠ get_performance_stats failed: {e}")
    
    # Close database
    try:
        db.close()
        print("✓ close method works")
    except Exception as e:
        print(f"⚠ close method failed: {e}")

except ImportError as e:
    print(f"✗ Failed to import Rust bindings: {e}")
except Exception as e:
    print(f"✗ Error during testing: {e}")

print("\nComprehensive test completed.")