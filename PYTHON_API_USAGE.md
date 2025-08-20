# miniGU Python API 使用说明

## 已修复的问题

### 1. INSERT语句格式问题
Python绑定中生成的INSERT语句格式已从错误的格式：
```
INSERT :Label { properties }
```
修复为正确的GQL格式：
```
INSERT (:Label { properties })
```

### 2. import/export过程调用问题
Python绑定中的import/export调用已修复：
- 移除了语句末尾的分号
- 使用正确的图名称参数
- 移除了不必要的`return *`后缀

### 3. show_procedures调用方法

#### 问题描述
`show_procedures`过程在直接调用时会失败：
```python
# 错误的调用方式
result = db.execute("CALL show_procedures()")
```

#### 正确的调用方式
需要添加`return *`来将目录修改过程作为查询过程使用：
```python
# 正确的调用方式
result = db.execute("CALL show_procedures() return *")
```

这会返回一个包含两列的结果集：
1. name: 过程名称
2. params: 参数类型列表（以逗号分隔）

#### 示例
```python
import minigu

with minigu.connect() as db:
    result = db.execute("CALL show_procedures() return *")
    print("可用的过程:")
    for row in result.data:
        print(f"  {row[0]}({row[1]})")
```

### 4. 已验证的功能

以下功能已通过测试验证可以正常工作：

1. **数据库连接和初始化**
   ```python
   db = minigu.connect()
   ```

2. **create_test_graph过程**
   ```python
   result = db.execute("CALL create_test_graph('graph_name')")
   ```

3. **import/export过程**
   ```python
   # 导出数据
   db.save("export_directory")
   
   # 导入数据
   db.load("export_directory")
   ```

4. **show_procedures过程（使用正确语法）**
   ```python
   result = db.execute("CALL show_procedures() return *")
   ```

### 5. 仍在开发中的功能

以下功能仍在开发中，当前使用会失败：

1. **MATCH查询语句** - 会导致程序恐慌
2. **USE GRAPH语句** - 解析错误
3. **INSERT语句** - 解析错误
4. **UPDATE/DELETE语句** - 未测试，可能有类似问题

### 6. 使用建议

1. 使用已验证的功能构建应用程序
2. 对于仍在开发中的功能，请耐心等待或使用模拟实现进行开发
3. 注意GQL语法要求，避免在语句末尾添加分号
4. 使用`CALL procedure_name() return *`语法调用返回数据的过程