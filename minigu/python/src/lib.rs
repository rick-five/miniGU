//! Python bindings for miniGU graph database
//!
//! This module provides Python bindings for the miniGU graph database using PyO3.

use arrow::array::*;
use arrow::datatypes::DataType;
use minigu::database::{Database, DatabaseConfig};
use minigu::session::Session;
use minigu_common::data_chunk::DataChunk;
use pyo3::prelude::*;
// Enable auto-initialize on macOS
#[cfg(feature = "auto-initialize")]
use pyo3::prepare_freethreaded_python;
use pyo3::types::{PyDict, PyList, PyModule, PyString};

/// PyMiniGu class that wraps the Rust Database
#[allow(non_local_definitions)]
#[pyclass]
#[allow(clippy::upper_case_acronyms)]
pub struct PyMiniGU {
    database: Option<Database>,
    session: Option<Session>,
    #[allow(dead_code)]
    db_path: Option<String>,
}

#[pymethods]
impl PyMiniGU {
    /// Create a new PyMiniGU instance
    #[new]
    fn new() -> PyResult<Self> {
        Ok(PyMiniGU {
            database: None,
            session: None,
            db_path: None,
        })
    }

    /// Initialize the database
    #[allow(unsafe_op_in_unsafe_fn)]
    fn init(&mut self) -> PyResult<()> {
        let config = DatabaseConfig::default();
        match Database::open_in_memory(&config) {
            Ok(db) => match db.session() {
                Ok(session) => {
                    self.database = Some(db);
                    self.session = Some(session);
                    Ok(())
                }
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                    "Failed to create session: {}",
                    e
                ))),
            },
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to initialize database: {}",
                e
            ))),
        }
    }

    /// Execute a GQL query
    #[allow(unsafe_op_in_unsafe_fn)]
    fn execute(&mut self, query: &str, py: Python) -> PyResult<PyObject> {
        // Get the session
        let session = self.session.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>("Session not initialized")
        })?;

        // Execute the query
        match session.query(query) {
            Ok(query_result) => {
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
                    let chunk_data = convert_data_chunk(chunk)?;
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
                metrics_dict
                    .set_item("parsing_time_ms", metrics.parsing_time().as_millis() as f64)?;
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
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Query execution failed: {}",
                e
            ))),
        }
    }

    /// Load data from a file
    #[allow(unsafe_op_in_unsafe_fn)]
    fn load_from_file(&mut self, path: &str) -> PyResult<()> {
        // Get the session
        let session = self.session.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>("Session not initialized")
        })?;

        // Execute the import procedure with correct syntax (no semicolon)
        let query = format!("CALL import('test_graph', '{}', 'manifest.json')", path);
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

    /// Load data directly
    #[allow(unsafe_op_in_unsafe_fn)]
    fn load_data(&mut self, data: &Bound<'_, PyAny>) -> PyResult<()> {
        // Get the session
        let session = self.session.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>("Session not initialized")
        })?;

        // Convert Python data to Rust data structures
        if let Ok(list) = data.downcast::<PyList>() {
            println!("Loading {} records", list.len());

            // Build GQL INSERT statements from the Python data
            let mut insert_statements = Vec::new();

            for item in list.iter() {
                if let Ok(dict) = item.downcast::<PyDict>() {
                    // Extract label and properties
                    let mut label = "Node".to_string();
                    let mut properties = Vec::new();

                    for (key, value) in dict.iter() {
                        if let (Ok(key_str), Ok(value_str)) = (
                            key.downcast::<PyString>().map(|s| s.to_string()),
                            value.str().map(|s| s.to_string()),
                        ) {
                            if key_str == "label" {
                                label = value_str;
                            } else {
                                // Format property value appropriately
                                // Based on GQL examples, we need to handle different types correctly
                                // For now, we'll try to determine if it's a number or string
                                if let Ok(int_val) = value_str.parse::<i64>() {
                                    properties.push(format!("{}: {}", key_str, int_val));
                                } else if let Ok(float_val) = value_str.parse::<f64>() {
                                    properties.push(format!("{}: {}", key_str, float_val));
                                } else {
                                    // It's a string, remove the extra quotes if they exist
                                    let clean_value = if value_str.starts_with('\'') && value_str.ends_with('\'') && value_str.len() > 1 {
                                        &value_str[1..value_str.len()-1]
                                    } else {
                                        &value_str
                                    };
                                    properties.push(format!("{}: '{}'", key_str, clean_value));
                                }
                            }
                        }
                    }

                    // Create INSERT statement using correct GQL syntax
                    if !properties.is_empty() {
                        let props_str = properties.join(", ");
                        // Use (:Label { properties }) syntax according to GQL specification
                        let statement = format!("INSERT (:{} {{ {} }})", label, props_str);
                        insert_statements.push(statement);
                    }
                }
            }

            // Execute all INSERT statements
            for statement in insert_statements {
                match session.query(&statement) {
                    Ok(_) => {
                        println!("Successfully executed: {}", statement);
                    }
                    Err(e) => {
                        return Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                            "Failed to execute statement '{}': {}",
                            statement, e
                        )));
                    }
                }
            }

            println!("All data loaded successfully");
            Ok(())
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyException, _>(
                "Expected a list of dictionaries",
            ))
        }
    }

    /// Save database to a file
    #[allow(unsafe_op_in_unsafe_fn)]
    fn save_to_file(&mut self, path: &str) -> PyResult<()> {
        // Get the session
        let session = self.session.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>("Session not initialized")
        })?;

        // Execute the export procedure with correct syntax (no semicolon)
        let query = format!("CALL export('test_graph', '{}', 'manifest.json')", path);
        match session.query(&query) {
            Ok(_) => {
                println!("Database saved successfully to: {}", path);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to save database to file: {}",
                e
            ))),
        }
    }

    /// Create a graph
    #[allow(unsafe_op_in_unsafe_fn)]
    fn create_graph(&mut self, name: &str, schema: Option<&str>) -> PyResult<()> {
        // Get the session
        let session = self.session.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>("Session not initialized")
        })?;

        // Create the graph using the create_test_graph procedure
        let query = format!("CALL create_test_graph('{}');", name);
        match session.query(&query) {
            Ok(_) => {
                println!("Graph '{}' created successfully", name);

                // If schema is provided, we could process it here
                if let Some(schema_str) = schema {
                    println!("Schema provided but not yet implemented: {}", schema_str);
                    // In a full implementation, we would parse the schema and add vertex/edge types
                }

                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to create graph '{}': {}",
                name, e
            ))),
        }
    }

    /// Insert data
    #[allow(unsafe_op_in_unsafe_fn)]
    fn insert_data(&mut self, data: &str) -> PyResult<()> {
        // Get the session
        let session = self.session.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>("Session not initialized")
        })?;

        // Execute the INSERT statement
        match session.query(data) {
            Ok(_) => {
                println!("Data inserted successfully: {}", data);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to insert data: {}",
                e
            ))),
        }
    }

    /// Update data
    #[allow(unsafe_op_in_unsafe_fn)]
    fn update_data(&mut self, query: &str) -> PyResult<()> {
        // Get the session
        let session = self.session.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>("Session not initialized")
        })?;

        // Execute the UPDATE statement
        match session.query(query) {
            Ok(_) => {
                println!("Data updated successfully with query: {}", query);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to update data: {}",
                e
            ))),
        }
    }

    /// Delete data
    #[allow(unsafe_op_in_unsafe_fn)]
    fn delete_data(&mut self, query: &str) -> PyResult<()> {
        // Get the session
        let session = self.session.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>("Session not initialized")
        })?;

        // Execute the DELETE statement
        match session.query(query) {
            Ok(_) => {
                println!("Data deleted successfully with query: {}", query);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "Failed to delete data: {}",
                e
            ))),
        }
    }

    /// Close the database connection
    fn close(&mut self) -> PyResult<()> {
        self.database = None;
        self.session = None;
        Ok(())
    }
}

/// Convert a DataChunk to a Python list of lists
fn convert_data_chunk(chunk: &DataChunk) -> PyResult<Vec<Vec<PyObject>>> {
    let mut result = Vec::new();

    // Get the number of rows
    let num_rows = chunk.len();

    // For each row, create a list of values
    for row_idx in 0..num_rows {
        let mut row_vec = Vec::new();

        // For each column, get the value at this row
        for col in chunk.columns() {
            let value = extract_value_from_array(col, row_idx)?;
            row_vec.push(value);
        }

        result.push(row_vec);
    }

    Ok(result)
}

/// Extract a value from an Arrow array at a specific index
fn extract_value_from_array(array: &ArrayRef, index: usize) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        match array.data_type() {
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                if arr.is_null(index) {
                    Ok(py.None())
                } else {
                    // Special handling for macOS to avoid linking issues
                    #[cfg(target_os = "macos")]
                    {
                        #[allow(deprecated)]
                        {
                            Ok(arr.value(index).into_py(py))
                        }
                    }
                    #[cfg(not(target_os = "macos"))]
                    {
                        Ok(arr.value(index).into_pyobject(py)?.into_any().unbind())
                    }
                }
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                if arr.is_null(index) {
                    Ok(py.None())
                } else {
                    // Special handling for macOS to avoid linking issues
                    #[cfg(target_os = "macos")]
                    {
                        #[allow(deprecated)]
                        {
                            Ok(arr.value(index).into_py(py))
                        }
                    }
                    #[cfg(not(target_os = "macos"))]
                    {
                        Ok(arr.value(index).into_pyobject(py)?.into_any().unbind())
                    }
                }
            }
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                if arr.is_null(index) {
                    Ok(py.None())
                } else {
                    #[allow(deprecated)]
                    {
                        Ok(arr.value(index).into_py(py))
                    }
                }
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                if arr.is_null(index) {
                    Ok(py.None())
                } else {
                    // Special handling for macOS to avoid linking issues
                    #[cfg(target_os = "macos")]
                    {
                        #[allow(deprecated)]
                        {
                            Ok(arr.value(index).into_py(py))
                        }
                    }
                    #[cfg(not(target_os = "macos"))]
                    {
                        Ok(arr.value(index).into_pyobject(py)?.into_any().unbind())
                    }
                }
            }
            _ => {
                // For unsupported types, convert to string representation
                // Special handling for macOS to avoid linking issues
                #[cfg(target_os = "macos")]
                {
                    #[allow(deprecated)]
                    {
                        Ok(format!("Unsupported type: {:?}", array.data_type()).into_py(py))
                    }
                }
                #[cfg(not(target_os = "macos"))]
                {
                    Ok(format!("Unsupported type: {:?}", array.data_type())
                        .into_pyobject(py)?
                        .into_any()
                        .unbind())
                }
            }
        }
    })
}

/// Python module definition
#[pymodule]
fn minigu_python(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMiniGU>()?;
    Ok(())
}
