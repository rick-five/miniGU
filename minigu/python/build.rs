fn main() {
    // Only run this build script when building for Python extension
    if std::env::var("CARGO_CFG_TARGET_OS").is_ok_and(|target_os| {
        target_os == "linux" || target_os == "windows" || target_os == "macos"
    }) {
        // Ensure we link against the Python library correctly
        pyo3_build_config::use_pyo3_cfgs();
    }

    // Print cargo metadata for pyo3
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=pyproject.toml");

    // Generate the Python extension module
    if let Ok(target_os) = std::env::var("CARGO_CFG_TARGET_OS") {
        match target_os.as_str() {
            "windows" => {
                // On Windows, we may need to handle special linking
                println!("cargo:rustc-link-lib=python3");
            }
            "macos" => {
                // On macOS, use framework linking
                println!("cargo:rustc-link-lib=framework=Python");
                // Add common framework search paths
                println!("cargo:rustc-link-search=framework=/opt/homebrew/Frameworks");
                println!("cargo:rustc-link-search=framework=/usr/local/Frameworks");
            }
            "linux" => {
                // On Linux, link against python3
                println!("cargo:rustc-link-lib=python3");
            }
            _ => {}
        }
    }

    // Enable PyO3 auto-initialize feature
    println!("cargo:rustc-cfg=pyo3_auto_initialize");
}
