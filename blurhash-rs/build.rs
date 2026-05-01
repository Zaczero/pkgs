use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

const LINEAR_TO_SRGB_LUT_LEN: usize = 4096;

fn srgb_u8_to_linear(v: u8) -> f32 {
    let srgb = f32::from(v) / 255.0;
    if srgb <= 0.04045 {
        srgb / 12.92
    } else {
        ((srgb + 0.055) / 1.055).powf(2.4)
    }
}

fn linear_to_srgb_u8(v: f32) -> u8 {
    let v = v.clamp(0.0, 1.0);
    let srgb = if v < 0.003_130_8 {
        v * 12.92
    } else {
        1.055_f32.mul_add(v.powf(1.0 / 2.4), -0.055)
    };
    let value = (srgb * 255.0 + 0.5) as i32;
    value.clamp(0, 255) as u8
}

fn separated_hex_u32(value: u32) -> String {
    let digits = format!("{value:08x}");
    format!("{}_{}", &digits[..4], &digits[4..])
}

fn main() {
    pyo3_build_config::use_pyo3_cfgs();
    println!("cargo:rerun-if-changed=build.rs");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set"));
    let dest = out_dir.join("srgb_tables.rs");

    let mut out = String::new();
    out.push_str("pub const SRGB_U8_TO_LINEAR: [f32; 256] = [\n");
    for v in 0_u16..=255 {
        let lin = srgb_u8_to_linear(v as u8);
        writeln!(
            out,
            "    f32::from_bits(0x{}),",
            separated_hex_u32(lin.to_bits())
        )
        .expect("writing to String cannot fail");
    }
    out.push_str("];\n\n");

    writeln!(
        out,
        "pub const LINEAR_TO_SRGB_U8: [u8; {LINEAR_TO_SRGB_LUT_LEN}] = ["
    )
    .expect("writing to String cannot fail");
    for i in 0..LINEAR_TO_SRGB_LUT_LEN {
        let v = i as f32 / (LINEAR_TO_SRGB_LUT_LEN - 1) as f32;
        let srgb = linear_to_srgb_u8(v);
        writeln!(out, "    {srgb},").expect("writing to String cannot fail");
    }
    out.push_str("];\n");

    fs::write(dest, out).expect("Failed to write generated sRGB tables");
}
