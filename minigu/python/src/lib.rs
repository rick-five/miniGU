//! Python bindings for miniGU graph database
//!
//! This module provides Python bindings for the miniGU graph database using PyO3.

// 只导入最基本的模块，避免在模块加载时进行复杂操作
use std::path::Path;

use arrow::array::*;
use arrow::datatypes::DataType;
use minigu::common::data_chunk::DataChunk;
use minigu::database::{Database, DatabaseConfig};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

// Define custom exception types
#[pyfunction]
fn is_syntax_error(e: &Bound<PyAny>) -> PyResult<bool> {
    // For now, we'll do a simple string check, but in a real implementation
    // we would check the actual error type from the Rust side
    let error_str: String = e.str()?.extract()?;
    Ok(error_str.to_lowercase().contains("syntax")
        || error_str.to_lowercase().contains("unexpected"))
}

#[pyfunction]
fn is_timeout_error(e: &Bound<PyAny>) -> PyResult<bool> {
    let error_str: String = e.str()?.extract()?;
    Ok(error_str.to_lowercase().contains("timeout"))
}

#[pyfunction]
fn is_transaction_error(e: &Bound<PyAny>) -> PyResult<bool> {
    let error_str: String = e.str()?.extract()?;
    Ok(error_str.to_lowercase().contains("transaction"))
}

#[pyfunction]
fn is_not_implemented_error(e: &Bound<PyAny>) -> PyResult<bool> {
    let error_str: String = e.str()?.extract()?;
    Ok(error_str.to_lowercase().contains("not implemented")
        || error_str.to_lowercase().contains("not yet implemented"))
}

/// PyMiniGU class that wraps the Rust Database
#[pyclass]
#[allow(clippy::upper_case_acronyms)]
pub struct PyMiniGU {
    // 不再存储任何数据库或会话相关的字段
    // 这样可以避免在模块导入时创建复杂对象导致的段错误
    initialized: bool,
    current_graph: Option<String>, // Track current graph name
}

#[pymethods]
impl PyMiniGU {
    /// Create a new PyMiniGU instance
    #[new]
    fn new() -> PyResult<Self> {
        // Only initialize fields to false/None, don't create database or session yet
        // This is critical to prevent segfaults during module import
        Ok(PyMiniGU {
            initialized: false,
            current_graph: None,
        })
    }

    /// Initialize the database
    fn init(&mut self) -> PyResult<()> {
        // Check if already initialized
        if self.initialized {
            println!("Database already initialized");
            return Ok(());
        }

        println!("Initializing database...");
        let config = DatabaseConfig::default();

        // Try to initialize database and session, but handle errors gracefully
        match Database::open_in_memory(&config) {
            Ok(_db) => {
                match _db.session() {
                    Ok(_) => {
                        // Mark as initialized
                        self.initialized = true;
                        self.current_graph = None;
                        println!("Session initialized successfully");
                        Ok(())
                    }
                    Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                        "Failed to create session: {}",
                        e
                    ))),
                }
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))),
        }
    }

    /// Execute a GQL query
    fn execute(&mut self, query_str: &str, py: Python) -> PyResult<PyObject> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        // 执行查询
        let query_result = session.query(query_str).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!("Query execution failed: {}", e))
        })?;

        // 转换查询结果到Python字典
        let dict = PyDict::new(py);

        // 转换schema
        let schema_list = PyList::empty(py);
        if let Some(schema_ref) = query_result.schema() {
            for field in schema_ref.fields() {
                let field_dict = PyDict::new(py);
                field_dict.set_item("name", field.name())?;
                field_dict.set_item("data_type", format!("{:?}", field.ty()))?;
                schema_list.append(field_dict)?;
            }
        }

        dict.set_item("schema", schema_list)?;

        // 转换数据
        let data_list = PyList::empty(py);
        for chunk in query_result.iter() {
            // 转换DataChunk到Python列表
            let chunk_data = convert_data_chunk(chunk).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                    "Failed to convert data chunk: {}",
                    e
                ))
            })?;
            for row in chunk_data {
                let row_list = PyList::empty(py);
                for value in row {
                    row_list.append(value)?;
                }
                data_list.append(row_list)?;
            }
        }

        dict.set_item("data", data_list)?;

        // 转换指标
        let metrics = query_result.metrics();
        let metrics_dict = PyDict::new(py);
        metrics_dict.set_item("parsing_time_ms", metrics.parsing_time().as_millis() as f64)?;
        metrics_dict.set_item(
            "planning_time_ms",
            metrics.planning_time().as_millis() as f64,
        )?;
        metrics_dict.set_item(
            "execution_time_ms",
            metrics.execution_time().as_millis() as f64,
        )?;

        dict.set_item("metrics", metrics_dict)?;

        Ok(dict.into())
    }

    /// Load data from a file
    fn load_data(&mut self, path: &str) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 验证文件路径
        let path_obj = Path::new(path);
        if !path_obj.exists() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "File not found: {}",
                path
            )));
        }

        // 清理路径以防止注入攻击
        let sanitized_path = path.replace(['\'', '"', ';', '\n', '\r'], "");

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        let query = format!("LOAD DATA FROM \"{}\" INTO Graph", sanitized_path);
        session.query(&query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to load data from file: {}",
                e
            ))
        })?;

        println!("Data loaded successfully from: {}", path);
        Ok(())
    }

    /// Load data from a CSV file
    fn load_csv(&mut self, path: &str) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 验证文件路径
        let path_obj = Path::new(path);
        if !path_obj.exists() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "File not found: {}",
                path
            )));
        }

        // 清理路径以防止注入攻击
        let sanitized_path = path.replace(['\'', '"', ';', '\n', '\r'], "");

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        let query = format!("LOAD CSV FROM \"{}\" INTO Graph", sanitized_path);
        session.query(&query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to load CSV from file: {}",
                e
            ))
        })?;

        println!("CSV data loaded successfully from: {}", path);
        Ok(())
    }

    /// Load data from a JSON file
    fn load_json(&mut self, path: &str) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 验证文件路径
        let path_obj = Path::new(path);
        if !path_obj.exists() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "File not found: {}",
                path
            )));
        }

        // 清理路径以防止注入攻击
        let sanitized_path = path.replace(['\'', '"', ';', '\n', '\r'], "");

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        let query = format!("LOAD JSON FROM \"{}\" INTO Graph", sanitized_path);
        session.query(&query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to load JSON from file: {}",
                e
            ))
        })?;

        println!("JSON data loaded successfully from: {}", path);
        Ok(())
    }

    /// Save database to a file
    fn save_to_file(&mut self, file_path: &str) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 清理路径以防止注入攻击
        let sanitized_path = file_path.replace(['\'', '"', ';', '\n', '\r'], "");

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        // 执行导出过程
        let query = format!(
            "CALL export('{}', '{}', 'manifest.json')",
            "Graph", sanitized_path
        );

        session.query(&query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to save database: {}",
                e
            ))
        })?;

        println!("Database saved successfully to: {}", file_path);
        Ok(())
    }

    /// Create a new graph
    #[pyo3(signature = (graph_name, _schema = None))]
    fn create_graph(&mut self, graph_name: &str, _schema: Option<&str>) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 验证图名称
        if graph_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name cannot be empty",
            ));
        }

        // 清理图名称以防止注入
        let sanitized_name = graph_name.replace(['\'', '"', ';', '\n', '\r'], "");

        // 验证清理后的图名称
        if sanitized_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name contains only invalid characters",
            ));
        }

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        // 使用create_test_graph过程创建图
        let query = format!("CALL create_test_graph('{}')", sanitized_name);
        println!("Attempting to execute query: {}", query);

        session.query(&query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create graph '{}': {}",
                sanitized_name, e
            ))
        })?;

        println!("Graph '{}' created successfully", sanitized_name);
        Ok(())
    }

    /// Drop a graph
    fn drop_graph(&mut self, graph_name: &str) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 验证图名称
        if graph_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name cannot be empty",
            ));
        }

        // 清理图名称
        let sanitized_name = graph_name.replace(|c: char| !c.is_alphanumeric() && c != '_', "");

        // 验证清理后的图名称
        if sanitized_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name contains only invalid characters",
            ));
        }

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        let query = format!("DROP GRAPH {}", sanitized_name);
        session.query(&query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to drop graph '{}': {}",
                sanitized_name, e
            ))
        })?;

        println!("Graph '{}' dropped successfully", sanitized_name);
        Ok(())
    }

    /// Use a graph
    fn use_graph(&mut self, graph_name: &str) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 验证图名称
        if graph_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name cannot be empty",
            ));
        }

        // 清理图名称
        let sanitized_name = graph_name.replace(['\'', '"', ';', '\n', '\r'], "");

        // 验证清理后的图名称
        if sanitized_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name contains only invalid characters",
            ));
        }

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        let query = format!("USE GRAPH {}", sanitized_name);
        session.query(&query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to use graph '{}': {}",
                sanitized_name, e
            ))
        })?;

        Ok(())
    }

    /// Begin a transaction
    fn begin_transaction(&mut self) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        // 执行BEGIN TRANSACTION语句
        let query = "BEGIN TRANSACTION";
        session.query(query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to begin transaction: {}",
                e
            ))
        })?;

        println!("Transaction begun successfully");
        Ok(())
    }

    /// Commit the current transaction
    fn commit(&mut self) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        // 执行COMMIT TRANSACTION语句
        let query = "COMMIT TRANSACTION";
        session.query(query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to commit transaction: {}",
                e
            ))
        })?;

        Ok(())
    }

    /// Rollback the current transaction
    fn rollback(&mut self) -> PyResult<()> {
        // 检查是否已初始化
        if !self.initialized {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "MiniGU not initialized. Call init() first.",
            ));
        }

        // 为每次操作创建新的数据库和会话实例
        let config = DatabaseConfig::default();
        let db = Database::open_in_memory(&config).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))
        })?;

        let mut session = db.session().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        // 执行ROLLBACK TRANSACTION语句
        let query = "ROLLBACK TRANSACTION";
        session.query(query).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to rollback transaction: {}",
                e
            ))
        })?;

        Ok(())
    }

    /// Close the database connection
    fn close(&mut self) -> PyResult<()> {
        // 重置初始化标志
        self.initialized = false;
        Ok(())
    }
}

/// Convert DataChunk to Vec<Vec<Option<String>>> for Python consumption
fn convert_data_chunk(chunk: &DataChunk) -> PyResult<Vec<Vec<Option<String>>>> {
    let mut result = Vec::new();

    // Handle empty chunks
    if chunk.cardinality() == 0 {
        return Ok(result);
    }

    // Safely get column count
    let column_count = chunk.columns().len();
    if column_count == 0 {
        return Ok(result);
    }

    // Initialize rows with proper error handling
    for row_idx in 0..chunk.cardinality() {
        let mut row = Vec::with_capacity(column_count);

        // Process each column for this row
        for col_idx in 0..column_count {
            // Safely access column with bounds checking
            let column = match chunk.columns().get(col_idx) {
                Some(col) => col,
                None => {
                    return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                        "Column index {} out of bounds",
                        col_idx
                    )));
                }
            };

            // Safely get value with proper error handling
            match get_value_from_array(column, row_idx) {
                Ok(value) => row.push(value),
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                        "Failed to get value at row {}, column {}: {}",
                        row_idx, col_idx, e
                    )));
                }
            }
        }

        result.push(row);
    }

    Ok(result)
}

/// Get a value from an Arrow array at the specified index and convert it to Option<String>
fn get_value_from_array(array: &dyn Array, index: usize) -> PyResult<Option<String>> {
    // Bounds check
    if index >= array.len() {
        return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
            "Index {} out of bounds for array of length {}",
            index,
            array.len()
        )));
    }

    // Handle null values
    if array.is_null(index) {
        return Ok(None);
    }

    // Match on data type and extract value
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(Some(arr.value(index).to_string()))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(Some(arr.value(index).to_string()))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Some(arr.value(index).to_string()))
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(Some(arr.value(index).to_string()))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(Some(arr.value(index).to_string()))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(Some(arr.value(index).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(Some(arr.value(index).to_string()))
        }
        _ => {
            // For unsupported types, return a descriptive string
            Ok(Some(format!("Unsupported type: {:?}", array.data_type())))
        }
    }
}

/// Python module definition
#[pymodule]
fn minigu_python(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // 只注册最基本的类和函数，避免在模块加载时执行任何复杂操作
    // 使用更安全的错误处理方式
    m.add_class::<PyMiniGU>().map_err(|e| {
        eprintln!("Failed to add PyMiniGU class: {:?}", e);
        e
    })?;
    m.add_function(wrap_pyfunction!(is_syntax_error, m)?)
        .map_err(|e| {
            eprintln!("Failed to add is_syntax_error function: {:?}", e);
            e
        })?;
    m.add_function(wrap_pyfunction!(is_timeout_error, m)?)
        .map_err(|e| {
            eprintln!("Failed to add is_timeout_error function: {:?}", e);
            e
        })?;
    m.add_function(wrap_pyfunction!(is_transaction_error, m)?)
        .map_err(|e| {
            eprintln!("Failed to add is_transaction_error function: {:?}", e);
            e
        })?;
    m.add_function(wrap_pyfunction!(is_not_implemented_error, m)?)
        .map_err(|e| {
            eprintln!("Failed to add is_not_implemented_error function: {:?}", e);
            e
        })?;

    Ok(())
}
