use std::env;

fn main() {
    // Enable PyO3's automatic linking feature
    pyo3_build_config::use_pyo3_cfgs();
    
    // Special handling for macOS
    if env::var("TARGET").is_ok_and(|target| target.contains("apple")) {
        // Try to automatically find and link Python
        // First try to use python3-config to get the library path
        if let Ok(lib_dir) = std::process::Command::new("python3-config")
            .arg("--ldflags")
            .output()
        {
            if lib_dir.status.success() {
                let output = String::from_utf8_lossy(&lib_dir.stdout);
                for arg in output.trim().split_whitespace() {
                    if arg.starts_with("-L") {
                        println!("cargo:rustc-link-search=native={}", &arg[2..]);
                    } else if arg.starts_with("-l") {
                        println!("cargo:rustc-link-lib={}", &arg[2..]);
                    }
                }
            }
        } else {
            // Fallback to linking python3 directly
            println!("cargo:rustc-link-lib=dylib=python3");
        }
    }
}