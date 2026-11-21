use std::collections::HashMap;

use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyDict, PyList},
};
mod parsers;

#[pyfunction]
#[pyo3(name = "parse_timeseries_generic")]
fn parse_timeseries_generic_py(
    xml_text: &Bound<'_, PyAny>,
    labels: Vec<String>,
    metadata: Vec<String>,
    period_name: &str,
) -> PyResult<HashMap<String, Vec<parsers::Data>>> {
    let labels: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
    let metadata: Vec<&str> = metadata.iter().map(|s| s.as_str()).collect();

    let parsed = match xml_text.downcast::<PyBytes>() {
        Ok(xml_bytes) => parsers::parse_timeseries_generic_bytes(xml_bytes.as_bytes(), labels, metadata, period_name),
        Err(_) => {
            let xml_text = xml_text.extract::<String>()?;
            parsers::parse_timeseries_generic(&xml_text, labels, metadata, period_name)
        }
    };

    parsed.map_err(|error| PyErr::new::<PyValueError, _>(error.to_string()))
}

fn xml_node_to_python<'py>(py: Python<'py>, node: &parsers::XmlNode) -> PyResult<Bound<'py, PyDict>> {
    let result = PyDict::new(py);
    result.set_item("name", &node.name)?;
    result.set_item("namespace", node.namespace.as_deref())?;
    result.set_item("text", node.text.as_deref())?;

    let attributes = PyList::empty(py);
    for attribute in &node.attributes {
        let item = PyDict::new(py);
        item.set_item("name", &attribute.name)?;
        item.set_item("namespace", attribute.namespace.as_deref())?;
        item.set_item("value", &attribute.value)?;
        attributes.append(item)?;
    }
    result.set_item("attributes", attributes)?;

    let children = PyList::empty(py);
    for child in &node.children {
        children.append(xml_node_to_python(py, child)?)?;
    }
    result.set_item("children", children)?;
    Ok(result)
}

#[pyfunction]
#[pyo3(name = "parse_xml")]
fn parse_xml_py(py: Python<'_>, xml_bytes: &Bound<'_, PyBytes>) -> PyResult<Py<PyAny>> {
    let node =
        parsers::parse_xml(xml_bytes.as_bytes()).map_err(|error| PyErr::new::<PyValueError, _>(error.to_string()))?;
    Ok(xml_node_to_python(py, &node)?.into_any().unbind())
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_timeseries_generic_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse_xml_py, m)?)?;
    Ok(())
}
