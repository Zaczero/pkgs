pub(crate) const fn plural(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

/// A wall-clock span: minutes and seconds from one minute, otherwise as `duration`.
pub(crate) fn seconds(ns: f64) -> String {
    let value = ns / 1e9;
    if value >= 60.0 {
        let total = value.round() as u64;
        format!("{}m {:02}s", total / 60, total % 60)
    } else {
        duration(ns)
    }
}

pub(crate) fn duration(ns: f64) -> String {
    let (value, unit) = if ns >= 1e9 {
        (ns / 1e9, "s")
    } else if ns >= 1e6 {
        (ns / 1e6, "ms")
    } else if ns >= 1e3 {
        (ns / 1e3, "µs")
    } else {
        (ns, "ns")
    };
    if value >= 10.0 {
        format!("{value:.1} {unit}")
    } else {
        format!("{value:.2} {unit}")
    }
}
