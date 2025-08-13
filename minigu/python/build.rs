use std::env;

fn main() {
    // Enable PyO3's automatic linking feature
    pyo3_build_config::use_pyo3_cfgs();
    
    // Special handling for macOS
    if env::var("TARGET").map_or(false, |target| target.contains("apple")) {
        // Try to automatically find and link Python
        println!("cargo:rustc-link-lib=dylib=python3");
    }
    
    // Print PyO3 configuration for debugging
    if env::var("PYO3_PRINT_CONFIG").is_ok() {
        pyo3_build_config::print_cfgs();
    }
}