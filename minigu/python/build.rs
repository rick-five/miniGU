use std::env;

fn main() {
    // Special handling for macOS
    if env::var("CARGO_CFG_TARGET_OS").map_or(false, |os| os == "macos") {
        // Try to find Python framework
        if env::var("PYO3_PYTHON").is_ok() {
            println!("cargo:rustc-link-lib=framework=Python");
            println!("cargo:rustc-link-search=framework=/opt/homebrew/Frameworks");
            println!("cargo:rustc-link-search=framework=/usr/local/Frameworks");
        }
    }

    // Enable PyO3 auto-initialize feature
    println!("cargo:rustc-cfg=pyo3_auto_initialize");
}
