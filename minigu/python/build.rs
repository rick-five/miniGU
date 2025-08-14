use std::env;

fn main() {
    // Special handling for macOS
    if env::var("CARGO_CFG_TARGET_OS").is_ok_and(|os| os == "macos") {
        // Try to find Python framework
        if let Ok(python_lib) = env::var("PYTHON_LIB") {
            // Use the provided library flags
            for flag in python_lib.split_whitespace() {
                if flag.starts_with("-L") {
                    println!("cargo:rustc-link-search=native={}", &flag[2..]);
                } else if flag.starts_with("-l") {
                    println!("cargo:rustc-link-lib={}", &flag[2..]);
                } else if flag.starts_with("-framework") {
                    println!("cargo:rustc-link-lib=framework={}", &flag[11..]);
                }
            }
        } else if env::var("PYO3_PYTHON").is_ok() {
            // Fallback to framework linking
            println!("cargo:rustc-link-lib=framework=Python");
            println!("cargo:rustc-link-search=framework=/opt/homebrew/Frameworks");
            println!("cargo:rustc-link-search=framework=/usr/local/Frameworks");
        }
    }

    // Enable PyO3 auto-initialize feature
    println!("cargo:rustc-cfg=pyo3_auto_initialize");
}
