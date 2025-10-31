//! Python bindings for miniGU graph database
//!
//! This module provides Python bindings for the miniGU graph database using PyO3.

// 只导入最基本的模块，避免在模块加载时进行复杂操作
use std::path::Path;

use arrow::array::*;
use arrow::datatypes::DataType;
use minigu::common::data_chunk::DataChunk;
use minigu::database::{Database, DatabaseConfig};
use minigu::session::Session;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};

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
    database: Option<Database>,
    session: Option<Session>,
    current_graph: Option<String>, // Track current graph name
}

#[pymethods]
impl PyMiniGU {
    /// Create a new PyMiniGU instance
    #[new]
    fn new() -> PyResult<Self> {
        // Only initialize fields to None, don't create database or session yet
        // This is critical to prevent segfaults during module import
        Ok(PyMiniGU {
            database: None,
            session: None,
            current_graph: None,
        })
    }

    /// Initialize the database
    fn init(&mut self) -> PyResult<()> {
        // Check if already initialized
        if self.database.is_some() && self.session.is_some() {
            println!("Database already initialized");
            return Ok(());
        }

        println!("Initializing database...");
        let config = DatabaseConfig::default();

        // Use safer error handling for database initialization
        let db = match Database::open_in_memory(&config) {
            Ok(db) => db,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                    "Failed to initialize database: {}",
                    e
                )));
            }
        };

        // Use safer error handling for session creation
        let session = match db.session() {
            Ok(session) => session,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                    "Failed to create session: {}",
                    e
                )));
            }
        };

        // Debug information
        println!("Session initialized");
        // Note: We can't access the private context field of Session here
        // The session is initialized and ready to use
        println!("Session is ready");

        self.database = Some(db);
        self.session = Some(session);
        self.current_graph = None;
        println!("Session initialized successfully");
        Ok(())
    }

    /// Execute a GQL query
    fn execute(&mut self, query_str: &str, py: Python) -> PyResult<PyObject> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Execute the query with proper error handling
        let query_result = match session.query(query_str) {
            Ok(result) => result,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                    "Query execution failed: {}",
                    e
                )));
            }
        };

        // Convert QueryResult to Python dict
        let dict = PyDict::new(py);

        // Convert schema
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

        // Convert data
        let data_list = PyList::empty(py);
        for chunk in query_result.iter() {
            // Convert DataChunk to Python list of lists
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

        // Convert metrics
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
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Validate file path
        let path_obj = Path::new(path);
        if !path_obj.exists() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "File not found: {}",
                path
            )));
        }

        // Use current graph or default to "default_graph"
        let graph_name = self.current_graph.as_deref().unwrap_or("default_graph");

        // Sanitize the path to prevent injection attacks
        let sanitized_path = path.replace(['\'', '"', ';', '\n', '\r'], "");

        let query = format!("LOAD DATA FROM \"{}\" INTO {}", sanitized_path, graph_name);
        match session.query(&query) {
            Ok(_) => {
                println!("Data loaded successfully from: {}", path);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to load data from file: {}",
                e
            ))),
        }
    }

    /// Load data from a CSV file
    fn load_csv(&mut self, path: &str) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Validate file path
        let path_obj = Path::new(path);
        if !path_obj.exists() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "File not found: {}",
                path
            )));
        }

        // Use current graph or default to "default_graph"
        let graph_name = self.current_graph.as_deref().unwrap_or("default_graph");

        // Sanitize the path to prevent injection attacks
        let sanitized_path = path.replace(['\'', '"', ';', '\n', '\r'], "");

        let query = format!("LOAD CSV FROM \"{}\" INTO {}", sanitized_path, graph_name);
        match session.query(&query) {
            Ok(_) => {
                println!("CSV data loaded successfully from: {}", path);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to load CSV from file: {}",
                e
            ))),
        }
    }

    /// Load data from a JSON file
    fn load_json(&mut self, path: &str) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Validate file path
        let path_obj = Path::new(path);
        if !path_obj.exists() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "File not found: {}",
                path
            )));
        }

        // Use current graph or default to "default_graph"
        let graph_name = self.current_graph.as_deref().unwrap_or("default_graph");

        // Sanitize the path to prevent injection attacks
        let sanitized_path = path.replace(['\'', '"', ';', '\n', '\r'], "");

        let query = format!("LOAD JSON FROM \"{}\" INTO {}", sanitized_path, graph_name);
        match session.query(&query) {
            Ok(_) => {
                println!("JSON data loaded successfully from: {}", path);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to load JSON from file: {}",
                e
            ))),
        }
    }

    /// Save database to a file
    fn save_to_file(&mut self, file_path: &str) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Use current graph or default to "default_graph"
        let graph_name = self.current_graph.as_deref().unwrap_or("default_graph");

        // Sanitize the path to prevent injection attacks
        let sanitized_path = file_path.replace(['\'', '"', ';', '\n', '\r'], "");

        // Execute export procedure with correct syntax (no semicolon)
        let query = format!(
            "CALL export('{}', '{}', 'manifest.json')",
            graph_name, sanitized_path
        );

        match session.query(&query) {
            Ok(_) => {
                println!("Database saved successfully to: {}", file_path);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to save database: {}",
                e
            ))),
        }
    }

    /// Create a new graph
    #[pyo3(signature = (graph_name, _schema = None))]
    fn create_graph(&mut self, graph_name: &str, _schema: Option<&str>) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Validate graph name
        if graph_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name cannot be empty",
            ));
        }

        // Sanitize the graph name to prevent injection
        let sanitized_name = graph_name.replace(['\'', '"', ';', '\n', '\r'], "");

        // Validate graph name after sanitization
        if sanitized_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name contains only invalid characters",
            ));
        }

        // Create the graph using the create_test_graph procedure
        let query = format!("CALL create_test_graph('{}')", sanitized_name);
        println!("Attempting to execute query: {}", query);

        match session.query(&query) {
            Ok(_) => {
                println!("Graph '{}' created successfully", sanitized_name);
                self.current_graph = Some(sanitized_name);
                Ok(())
            }
            Err(e) => {
                println!("Error executing query '{}': {}", query, e);
                Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                    "Failed to create graph '{}': {}",
                    sanitized_name, e
                )))
            }
        }
    }

    /// Drop a graph
    fn drop_graph(&mut self, graph_name: &str) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Validate graph name
        if graph_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name cannot be empty",
            ));
        }

        // Sanitize graph name
        let sanitized_name = graph_name.replace(|c: char| !c.is_alphanumeric() && c != '_', "");

        // Validate graph name after sanitization
        if sanitized_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name contains only invalid characters",
            ));
        }

        let query = format!("DROP GRAPH {}", sanitized_name);
        match session.query(&query) {
            Ok(_) => {
                // Clear current graph if it's the one being dropped
                if self.current_graph.as_deref() == Some(&sanitized_name) {
                    self.current_graph = None;
                }
                println!("Graph '{}' dropped successfully", sanitized_name);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to drop graph '{}': {}",
                sanitized_name, e
            ))),
        }
    }

    /// Use a graph
    fn use_graph(&mut self, graph_name: &str) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Validate graph name
        if graph_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name cannot be empty",
            ));
        }

        // Sanitize graph name
        let sanitized_name = graph_name.replace(['\'', '"', ';', '\n', '\r'], "");

        // Validate graph name after sanitization
        if sanitized_name.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Graph name contains only invalid characters",
            ));
        }

        let query = format!("USE GRAPH {}", sanitized_name);
        match session.query(&query) {
            Ok(_) => {
                self.current_graph = Some(sanitized_name);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to use graph '{}': {}",
                sanitized_name, e
            ))),
        }
    }

    /// Begin a transaction
    fn begin_transaction(&mut self) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Execute BEGIN TRANSACTION statement using correct GQL syntax
        let query = "BEGIN TRANSACTION";
        match session.query(query) {
            Ok(_) => {
                println!("Transaction begun successfully");
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to begin transaction: {}",
                e
            ))),
        }
    }

    /// Commit the current transaction
    fn commit(&mut self) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Execute COMMIT TRANSACTION statement using correct GQL syntax
        let query = "COMMIT TRANSACTION";
        match session.query(query) {
            Ok(_) => Ok(()),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to commit transaction: {}",
                e
            ))),
        }
    }

    /// Rollback the current transaction
    fn rollback(&mut self) -> PyResult<()> {
        // Get the session with proper error handling
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                    "Session not initialized. Call init() first.",
                ));
            }
        };

        // Execute ROLLBACK TRANSACTION statement using correct GQL syntax
        let query = "ROLLBACK TRANSACTION";
        match session.query(query) {
            Ok(_) => Ok(()),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to rollback transaction: {}",
                e
            ))),
        }
    }

    /// Get the error type for the last operation
    fn get_last_error_type(&self, e: &Bound<PyAny>) -> PyResult<String> {
        let error_str: String = e.str()?.extract()?;
        Ok(error_str)
    }

    /// Close the database connection
    fn close(&mut self) -> PyResult<()> {
        self.database = None;
        self.session = None;
        self.current_graph = None;
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
