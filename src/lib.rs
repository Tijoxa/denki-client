use pyo3::prelude::*;
mod parsers;

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
    // m.add_function(wrap_pyfunction!(parsers::parse_timeseries_generic_whole, m)?)?;
    Ok(())
}
