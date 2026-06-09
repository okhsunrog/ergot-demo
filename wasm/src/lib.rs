mod duplex;
mod node;

pub use node::*;

use wasm_bindgen::prelude::*;

/// Initialize console logging. `level` is one of "trace", "debug", "info",
/// "warn", "error". Call once, before creating nodes.
#[wasm_bindgen(js_name = initLogging)]
pub fn init_logging(level: &str) -> Result<(), JsError> {
    let level = match level {
        "trace" => log::Level::Trace,
        "debug" => log::Level::Debug,
        "info" => log::Level::Info,
        "warn" => log::Level::Warn,
        "error" => log::Level::Error,
        other => return Err(JsError::new(&format!("unknown log level: {other}"))),
    };
    console_log::init_with_level(level).map_err(|e| JsError::new(&e.to_string()))
}
