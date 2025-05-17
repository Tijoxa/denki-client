use std::collections::HashMap;

use pyo3::{exceptions::PyValueError, prelude::*};
mod parsers;

#[pyfunction]
#[pyo3(name = "parse_timeseries_generic")]
fn parse_timeseries_generic_py(
    xml_text: &str,
    label: &str,
    period_name: &str,
) -> PyResult<HashMap<String, Vec<String>>> {
    parsers::parse_timeseries_generic(xml_text, label, period_name)
        .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyfn(m)]
    fn hello_from_bin() -> String {
        "Hello from entsoe-rs!".to_string()
    }

    // m.add_function(wrap_pyfunction!(parsers::parse_timeseries_generic, m)?)?;
    m.add_function(wrap_pyfunction!(parse_timeseries_generic_py, m)?)?;
    Ok(())
}
