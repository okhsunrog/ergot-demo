use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

/// Greet someone by name.
#[wasm_bindgen]
pub fn greet(name: &str) -> String {
    format!("Hello, {name}! This message comes from Rust WASM.")
}

/// Compute the nth Fibonacci number.
#[wasm_bindgen]
pub fn fibonacci(n: u32) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        _ => {
            let (mut a, mut b) = (0u64, 1u64);
            for _ in 2..=n {
                let tmp = a + b;
                a = b;
                b = tmp;
            }
            b
        }
    }
}

/// Check whether a number is prime.
fn is_prime(n: u64) -> bool {
    if n < 2 {
        return false;
    }
    if n < 4 {
        return true;
    }
    if n % 2 == 0 || n % 3 == 0 {
        return false;
    }
    let mut i = 5;
    while i * i <= n {
        if n % i == 0 || n % (i + 2) == 0 {
            return false;
        }
        i += 6;
    }
    true
}

/// Exported wrapper for single prime checks from JS.
#[wasm_bindgen]
pub fn check_prime(n: u64) -> bool {
    is_prime(n)
}

/// Handle to a running background task. Call `stop()` to cancel it.
#[wasm_bindgen]
pub struct BackgroundTask {
    running: Rc<Cell<bool>>,
}

#[wasm_bindgen]
impl BackgroundTask {
    /// Signal the task to stop after the current batch.
    pub fn stop(&self) {
        self.running.set(false);
    }
}

/// Start a background prime search.
///
/// Checks numbers sequentially starting from 2. After each batch the
/// `on_progress` callback is invoked with (checked, primes_found, last_prime).
/// Returns a `BackgroundTask` handle whose `stop()` method cancels the search.
#[wasm_bindgen]
pub fn start_prime_search(on_progress: js_sys::Function) -> BackgroundTask {
    let running = Rc::new(Cell::new(true));
    let flag = running.clone();

    spawn_local(async move {
        let mut n: u64 = 2;
        let mut found: u64 = 0;
        let mut last_prime: u64 = 0;
        const BATCH: u64 = 50_000;

        while flag.get() {
            for _ in 0..BATCH {
                if !flag.get() {
                    break;
                }
                if is_prime(n) {
                    found += 1;
                    last_prime = n;
                }
                n += 1;
            }

            // Report progress to JS.
            let _ = on_progress.call3(
                &JsValue::null(),
                &JsValue::from_f64(n as f64),
                &JsValue::from_f64(found as f64),
                &JsValue::from_f64(last_prime as f64),
            );

            // Yield to the browser event loop so the UI stays responsive.
            tokio_with_wasm::time::sleep(Duration::ZERO).await;
        }
    });

    BackgroundTask { running }
}
