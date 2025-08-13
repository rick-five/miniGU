use std::env;

fn main() {
    // Enable PyO3's automatic linking feature
    pyo3_build_config::use_pyo3_cfgs();
    
    // Special handling for macOS
    if env::var("TARGET").is_ok_and(|target| target.contains("apple")) {
        // Try to automatically find and link Python
        println!("cargo:rustc-link-lib=dylib=python3");
    }
}