#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for graph data structures
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import minigu


def test_graph_data_structures():
    """Test graph data structures"""
    print("=== Testing Graph Data Structures ===")
    
    try:
        # Test creating nodes
        print("1. Creating nodes...")
        node1 = minigu.Node("Person", {"name": "Alice", "age": 30})
        node2 = minigu.Node("Person", {"name": "Bob", "age": 25})
        node3 = minigu.Node("Company", {"name": "TechCorp", "founded": 2010})
        print(f"   [PASS] Created nodes: {node1}, {node2}, {node3}")
        
        # Test creating edges
        print("2. Creating edges...")
        edge1 = minigu.Edge("KNOWS", node1, node2, {"since": 2020})
        edge2 = minigu.Edge("WORKS_FOR", node1, node3, {"position": "Engineer"})
        print(f"   [PASS] Created edges: {edge1}, {edge2}")
        
        # Test creating paths
        print("3. Creating paths...")
        path = minigu.Path([node1, node2], [edge1])
        print(f"   [PASS] Created path: {path}")
        
        # Test using the API to create these objects
        print("4. Using API methods to create objects...")
        with minigu.connect() as db:
            node_api = db.create_node("Person", {"name": "Charlie", "age": 35})
            edge_api = db.create_edge("KNOWS", node_api, node1, {"since": 2021})
            path_api = db.create_path([node_api, node1], [edge_api])
            print(f"   [PASS] Created via API: {node_api}, {edge_api}, {path_api}")
        
        print("\n[PASS] All graph data structure tests passed!")
        
    except Exception as e:
        print(f"[FAIL] Error during graph data structure tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Python API Graph Data Structures Test")
    print("=" * 50)
    
    try:
        test_graph_data_structures()
        
        print("\n" + "=" * 50)
        print("All graph data structure tests completed successfully!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()