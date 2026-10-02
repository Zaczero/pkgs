fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    pyo3_build_config::use_pyo3_cfgs();
}
