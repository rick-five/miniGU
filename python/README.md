# miniGU Python API

miniGU Python API 是一个为 [miniGU](https://github.com/TuGraph-family/miniGU) 图数据库设计的 Python 接口，使 Python 用户能够轻松地与 miniGU 数据库进行交互。

## 特性

- 简单易用的 API 设计
- 支持连接本地或内存数据库
- 执行 GQL 查询语句
- 加载和保存数据
- 获取格式化的查询结果
- 提供查询性能指标

## 安装

### 使用预构建版本

```bash
# 克隆项目
git clone https://github.com/TuGraph-family/miniGU.git

# 进入Python API目录
cd miniGU/minigu/python

# 安装Python包
pip install .
```

### 从源码构建

```bash
# 克隆项目
git clone https://github.com/TuGraph-family/miniGU.git

# 进入项目目录
cd miniGU

# 确保已安装Rust工具链
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 构建Python绑定
cd minigu/python
python build.py

# 安装Python包
pip install .
```

## 快速开始

### 基本用法

```python
from minigu import connect

# 连接到内存数据库
db = connect()

# 加载数据
data = [
    {"name": "Alice", "age": 30, "label": "Person"},
    {"name": "Bob", "age": 25, "label": "Person"}
]
db.load(data)

# 执行查询
result = db.execute("MATCH (n:Person) RETURN n.name, n.age;")

# 处理结果
print(f"查询返回 {result.row_count} 行数据")

# 将结果转换为字典列表
data = result.to_list()
for row in data:
    print(row)

# 保存数据库
db.save("my_database.mgu")

# 关闭连接
db.close()
```

### 使用上下文管理器

```python
from minigu import connect

# 使用上下文管理器自动管理连接
with connect("example.db") as db:
    # 加载数据
    db.load("data.json")
    
    # 执行查询
    result = db.execute("MATCH (n:Person) RETURN n.name, n.age;")
    print(result.to_dict())
    
    # 保存数据库
    db.save("backup.mgu")
# 连接会自动关闭
```

## API 参考

### `connect(db_path: Optional[str] = None) -> MiniGU`

创建到 miniGU 数据库的连接。

**参数:**
- `db_path`: 数据库文件路径，如果为 None 则创建内存数据库

**返回:**
- `MiniGU` 实例

### `MiniGU` 类

#### `execute(query: str) -> QueryResult`

执行 GQL 查询。

**参数:**
- `query`: GQL 查询语句

**返回:**
- `QueryResult` 查询结果对象

#### `load(data: Union[List[Dict], str]) -> None`

加载数据到数据库。

**参数:**
- `data`: 要加载的数据，可以是字典列表或文件路径

#### `save(path: str) -> None`

将数据库保存到指定路径。

**参数:**
- `path`: 保存路径

#### `close() -> None`

关闭数据库连接。

### `QueryResult` 类

#### `to_dict() -> Dict[str, Any]`

将结果转换为字典格式。

#### `to_list() -> List[Dict[str, Any]]`

将结果转换为字典列表格式。

**返回:**
- 每行数据作为一个字典的列表

#### 属性

- `schema`: 结果集的模式信息
- `data`: 查询结果数据
- `metrics`: 查询性能指标
- `row_count`: 结果行数

## 示例

### 创建节点和关系

```python
from minigu import connect

with connect() as db:
    # 创建节点数据
    person_data = [
        {"name": "Alice", "age": 30, "label": "Person"},
        {"name": "Bob", "age": 25, "label": "Person"},
        {"name": "TechCorp", "industry": "Technology", "label": "Company"}
    ]
    
    # 加载数据
    db.load(person_data)
    
    # 创建关系的GQL语句
    db.execute("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b);")
    db.execute("MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) CREATE (a)-[:WORKS_AT]->(c);")
    
    # 查询节点
    result = db.execute("MATCH (n:Person) RETURN n.name, n.age;")
    for row in result.to_list():
        print(f"Person: {row['n.name']}, Age: {row['n.age']}")

### 从文件加载数据

```python
from minigu import connect

with connect() as db:
    # 从JSON文件加载数据
    db.load("social_network_data.json")
    
    # 执行分析查询
    result = db.execute("""
        MATCH (p:Person)-[:FRIEND]->(f:Person)
        RETURN p.name, count(f) as friend_count
        ORDER BY friend_count DESC
        LIMIT 5
    """)
    
    print("Top 5 most connected people:")
    for row in result.to_list():
        print(f"  {row['p.name']}: {row['friend_count']} friends")
```