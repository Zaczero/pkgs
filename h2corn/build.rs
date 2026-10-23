fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(h2corn_pgo_collect)");
    pyo3_build_config::use_pyo3_cfgs();
}
