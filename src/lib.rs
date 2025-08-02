use std::collections::HashMap;

use pyo3::{exceptions::PyValueError, prelude::*};
mod parsers;

#[pyfunction]
#[pyo3(name = "parse_timeseries_generic")]
fn parse_timeseries_generic_py(
    xml_text: &str,
    labels: Vec<String>,
    metadata: Vec<String>,
    period_name: &str,
) -> PyResult<HashMap<String, Vec<parsers::Data>>> {
    let labels: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
    let metadata: Vec<&str> = metadata.iter().map(|s| s.as_str()).collect();

    parsers::parse_timeseries_generic(xml_text, labels, metadata, period_name)
        .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyfn(m)]
    fn hello_from_bin() -> String {
        "Hello from denki-rs!".to_string()
    }

    m.add_function(wrap_pyfunction!(parse_timeseries_generic_py, m)?)?;
    Ok(())
}
