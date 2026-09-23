//! PEP 3118 buffer byte-order normalization for typed numeric ingest.
//!
//! `PyBuffer::<T>::get` accepts non-native-endian formats when the element
//! size matches (pyo3's endian check is incorrect for little-endian hosts:
//! it treats `>` as matching). Raw reinterpret and `PyBuffer_ToContiguous`
//! never byteswap, so a big-endian `>f8`/`>u8` array is silently misread as
//! host-endian. Callers that want host-native values must normalize.

use std::ffi::CStr;

use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
#[cfg(Py_GIL_DISABLED)]
use pyo3::types::PyBytes;
#[cfg(Py_GIL_DISABLED)]
use pyo3::types::PyMemoryView;

#[cfg(Py_GIL_DISABLED)]
use crate::py::vectors::Float64Buffer;

/// Select direct typed loads instead of the generic buffer copy.
///
/// The GIL build retains the direct path for compatibility and throughput.
/// That is a capture-strategy choice, not a synchronization guarantee: a
/// native writer can release the GIL and modify the exporter concurrently.
/// Free-threaded builds use the narrower exact immutable-owner set for direct
/// loads. Every capture path still requires the producer to remain quiescent
/// unless the producer supplies its own synchronization protocol.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    expect(
        clippy::missing_const_for_fn,
        clippy::needless_return,
        reason = "the GIL build keeps the existing direct path while the free-threaded build walks the owner chain"
    )
)]
pub(crate) fn exporter_supports_direct_capture<T>(py: Python<'_>, buffer: &PyBuffer<T>) -> bool {
    #[cfg(not(Py_GIL_DISABLED))]
    {
        let _ = (py, buffer);
        return true;
    }
    #[cfg(Py_GIL_DISABLED)]
    {
        let Some(exporter) = buffer.obj(py) else {
            return false;
        };
        let mut owner = exporter.clone().unbind();
        // PyBuffer reports a memoryview as the exporter for typed views. Only
        // follow the exact built-in memoryview's own obj link; never invoke an
        // attribute supplied by an arbitrary exporter.
        loop {
            let owner_bound = owner.bind(py);
            if owner_bound.is_exact_instance_of::<PyBytes>()
                || owner_bound.is_exact_instance_of::<Float64Buffer>()
            {
                return true;
            }
            if !owner_bound.is_exact_instance_of::<PyMemoryView>() {
                return false;
            }
            let Ok(next) = owner_bound.getattr("obj") else {
                return false;
            };
            owner = next.unbind();
        }
    }
}

/// True when the buffer format describes host-native byte order.
///
/// Leading markers per the struct module / PEP 3118: `@` and `=` are native;
/// `<` is little-endian; `>` and `!` are big-endian. No marker (e.g. `"d"`,
/// `"Q"`) means native size-and-order.
pub(crate) const fn buffer_format_is_native_endian(format: &CStr) -> bool {
    match format.to_bytes().first() {
        Some(&b'<') => cfg!(target_endian = "little"),
        Some(&(b'>' | b'!')) => cfg!(target_endian = "big"),
        // no marker, `@`, `=`, or anything else treated as native (typed
        // PyBuffer already constrained the element kind)
        _ => true,
    }
}

pub(crate) const fn swap_f64_endian(value: f64) -> f64 {
    f64::from_bits(value.to_bits().swap_bytes())
}

pub(crate) const fn swap_u64_endian(value: u64) -> u64 {
    value.swap_bytes()
}

/// Copy a C-contiguous `f64` buffer into a `Vec<f64>` in host-native order.
///
/// Reads each element through [`pyo3::buffer::ReadOnlyCell`], forming a Rust
/// slice over the exporter's storage. The direct/copy distinction is not a
/// synchronization protocol: both paths require the producer to remain
/// quiescent for this finite capture. An exporter that does not support direct
/// capture takes CPython's ordinary contiguous copy instead.
///
/// # Capture contract
/// Caller must only pass a C-contiguous `f64` buffer held for this scope, and
/// its producer must remain quiescent until the copy returns.
pub(crate) fn buffer_to_column_f64(py: Python<'_>, buffer: &PyBuffer<f64>) -> PyResult<Vec<f64>> {
    if !exporter_supports_direct_capture(py, buffer) {
        return buffer_to_vec_f64(py, buffer);
    }
    let len = buffer.item_count();
    // A third-party exporter can provide inconsistent buffer metadata. Preserve
    // the public error path rather than turning that inconsistency into a panic.
    let Some(cells) = buffer.as_slice(py) else {
        return buffer_to_vec_f64(py, buffer);
    };
    if cells.len() != len {
        return buffer_to_vec_f64(py, buffer);
    }
    let mut values = vec![0.0; len];
    if buffer_format_is_native_endian(buffer.format()) {
        for (value, cell) in values.iter_mut().zip(cells.iter()) {
            *value = cell.get();
        }
    } else {
        for (value, cell) in values.iter_mut().zip(cells.iter()) {
            *value = swap_f64_endian(cell.get());
        }
    }
    Ok(values)
}

/// Copy buffer elements to a `Vec<f64>` in host-native order (byteswaps when
/// the PEP 3118 format is non-native).
///
/// This is an ordinary copy, not a synchronization protocol. The buffer
/// exporter must remain quiescent until the copy returns; retaining the
/// `PyBuffer` pins its allocation but does not exclude native writers.
pub(crate) fn buffer_to_vec_f64(py: Python<'_>, buffer: &PyBuffer<f64>) -> PyResult<Vec<f64>> {
    let mut values = vec![0.0; buffer.item_count()];
    buffer.copy_to_slice(py, &mut values)?;
    if !buffer_format_is_native_endian(buffer.format()) {
        for value in &mut values {
            *value = swap_f64_endian(*value);
        }
    }
    Ok(values)
}

/// Copy buffer elements to a `Vec<u64>` in host-native order.
///
/// This is an ordinary copy, not a synchronization protocol. The buffer
/// exporter must remain quiescent until the copy returns; retaining the
/// `PyBuffer` pins its allocation but does not exclude native writers.
pub(crate) fn buffer_to_vec_u64(py: Python<'_>, buffer: &PyBuffer<u64>) -> PyResult<Vec<u64>> {
    let mut values = vec![0; buffer.item_count()];
    buffer.copy_to_slice(py, &mut values)?;
    if !buffer_format_is_native_endian(buffer.format()) {
        for value in &mut values {
            *value = swap_u64_endian(*value);
        }
    }
    Ok(values)
}

/// `copy_to_slice` then byteswap if the buffer format is non-native.
///
/// The exporter must remain quiescent until the copy returns. A destination
/// slice is exclusive after the copy, but `copy_to_slice` does not synchronize
/// the source with a native writer.
pub(crate) fn buffer_copy_to_slice_u64(
    py: Python<'_>,
    buffer: &PyBuffer<u64>,
    target: &mut [u64],
) -> PyResult<()> {
    buffer.copy_to_slice(py, target)?;
    if !buffer_format_is_native_endian(buffer.format()) {
        for value in target.iter_mut() {
            *value = swap_u64_endian(*value);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;

    #[test]
    fn format_endian_markers() {
        let native_bare = CString::new("d").unwrap();
        let at = CString::new("@d").unwrap();
        let eq = CString::new("=d").unwrap();
        let le = CString::new("<d").unwrap();
        let be = CString::new(">d").unwrap();
        let net = CString::new("!d").unwrap();
        assert!(buffer_format_is_native_endian(&native_bare));
        assert!(buffer_format_is_native_endian(&at));
        assert!(buffer_format_is_native_endian(&eq));
        assert_eq!(
            buffer_format_is_native_endian(&le),
            cfg!(target_endian = "little")
        );
        assert_eq!(
            buffer_format_is_native_endian(&be),
            cfg!(target_endian = "big")
        );
        assert_eq!(
            buffer_format_is_native_endian(&net),
            cfg!(target_endian = "big")
        );
    }

    #[test]
    fn swap_f64_recovers_misread_be_nan_inf() {
        // Public geometry paths reject non-finite; prove the normalize step
        // itself: raw-reinterpret of big-endian NaN/Inf bytes as host f64
        // yields a finite garbage value; one endian swap restores non-finite.
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let be_bytes = value.to_be_bytes();
            let misread = f64::from_ne_bytes(be_bytes);
            if value.is_nan() {
                assert!(
                    swap_f64_endian(misread).is_nan(),
                    "misread of BE NaN should normalize to NaN"
                );
            } else {
                // BE Inf misread as native is a tiny finite on little-endian.
                assert!(
                    misread.is_finite() || misread.to_bits() == value.to_bits(),
                    "expected BE Inf misread to differ or already match"
                );
                let recovered = swap_f64_endian(misread);
                assert_eq!(recovered.to_bits(), value.to_bits());
            }
        }
        // Finite identity under double-swap (bit-exact).
        let x = 1.0_f64;
        assert_eq!(swap_f64_endian(swap_f64_endian(x)).to_bits(), x.to_bits());
    }

    #[test]
    fn swap_u64_recovers_misread_be_ids() {
        let id: u64 = 0x1400_0000_0000_0000;
        let misread = u64::from_ne_bytes(id.to_be_bytes());
        assert_ne!(misread, id);
        assert_eq!(swap_u64_endian(misread), id);
    }
}
