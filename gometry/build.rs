fn main() {
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        println!("cargo::rustc-link-lib=ole32");
        println!("cargo::rustc-link-lib=shell32");
    }
}
