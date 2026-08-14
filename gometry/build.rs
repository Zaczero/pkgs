fn main() {
    pyo3_build_config::use_pyo3_cfgs();
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        println!("cargo::rustc-link-lib=ole32");
        println!("cargo::rustc-link-lib=shell32");
    }
}
