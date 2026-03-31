use wasm_bindgen::prelude::*;

/// Verify ergot types are accessible from WASM.
#[wasm_bindgen]
pub fn ergot_info() -> String {
    // Just verify we can use core ergot types
    let _addr = ergot::Address {
        network_id: 1,
        node_id: 2,
        port_id: 0,
    };
    format!("ergot available")
}
