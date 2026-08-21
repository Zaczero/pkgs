//! PEP 3118 buffer byte-order normalization for typed numeric ingest.
//!
//! `PyBuffer::<T>::get` accepts non-native-endian formats when the element
//! size matches (pyo3's endian check is incorrect for little-endian hosts:
//! it treats `>` as matching). Raw reinterpret and `PyBuffer_ToContiguous`
//! never byteswap, so a big-endian `>f8`/`>u8` array is silently misread as
//! host-endian. Callers that want host-native values must normalize.

use std::ffi::CStr;
use std::mem::MaybeUninit;
use std::sync::Arc;

use pyo3::buffer::PyBuffer;
#[cfg(Py_GIL_DISABLED)]
use pyo3::exceptions::PyBufferError;
use pyo3::prelude::*;
#[cfg(Py_GIL_DISABLED)]
use pyo3::types::PyBytes;
#[cfg(Py_GIL_DISABLED)]
use pyo3::types::PyMemoryView;

#[cfg(Py_GIL_DISABLED)]
use crate::py::vectors::Float64Buffer;

/// Admit only exporters whose storage is immutable for the duration of a
/// native capture. A read-only view can still be a facade over writable
/// storage, so readonly metadata is not an admission proof.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    expect(
        clippy::missing_const_for_fn,
        clippy::needless_return,
        reason = "the GIL build always answers true while the free-threaded build walks the chain"
    )
)]
pub(crate) fn exporter_storage_is_stable<T>(
    py: Python<'_>,
    buffer: &PyBuffer<T>,
) -> bool {
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

/// Copy a C-contiguous `f64` buffer into an `Arc<[f64]>` in host-native order.
///
/// Reads each element through [`pyo3::buffer::ReadOnlyCell`], forming a Rust
/// slice over the exporter's storage. That is only sound while nothing else can
/// write it, so an exporter failing [`exporter_storage_is_stable`] takes
/// CPython's own contiguous copy instead: the same answer, no data race.
///
/// # Safety contract
/// Caller must only pass a C-contiguous `f64` buffer held for this scope.
pub(crate) fn buffer_to_arc_f64(
    py: Python<'_>,
    buffer: &PyBuffer<f64>,
) -> PyResult<Arc<[f64]>> {
    if !exporter_storage_is_stable(py, buffer) {
        return buffer_to_vec_f64(py, buffer).map(Into::into);
    }
    let len = buffer.item_count();
    let mut arc: Arc<[MaybeUninit<f64>]> = Arc::new_uninit_slice(len);
    let uninit = Arc::get_mut(&mut arc).expect("fresh unique Arc");
    // C-contiguous path: as_slice is always Some for a matching contiguous buffer.
    let cells = buffer
        .as_slice(py)
        .expect("C-contiguous f64 PyBuffer must expose as_slice");
    debug_assert_eq!(cells.len(), len);
    if buffer_format_is_native_endian(buffer.format()) {
        for (dst, cell) in uninit.iter_mut().zip(cells.iter()) {
            dst.write(cell.get());
        }
    } else {
        for (dst, cell) in uninit.iter_mut().zip(cells.iter()) {
            dst.write(swap_f64_endian(cell.get()));
        }
    }
    // SAFETY: every slot was written above.
    Ok(unsafe { arc.assume_init() })
}

/// Copy buffer elements to a `Vec<f64>` in host-native order (byteswaps when
/// the PEP 3118 format is non-native).
pub(crate) fn buffer_to_vec_f64(
    py: Python<'_>,
    buffer: &PyBuffer<f64>,
) -> PyResult<Vec<f64>> {
    let mut values = buffer.to_vec(py)?;
    if !buffer_format_is_native_endian(buffer.format()) {
        for value in &mut values {
            *value = swap_f64_endian(*value);
        }
    }
    Ok(values)
}

/// Copy buffer elements to a `Vec<u64>` in host-native order.
pub(crate) fn buffer_to_vec_u64(
    py: Python<'_>,
    buffer: &PyBuffer<u64>,
) -> PyResult<Vec<u64>> {
    let mut values = buffer.to_vec(py)?;
    if !buffer_format_is_native_endian(buffer.format()) {
        for value in &mut values {
            *value = swap_u64_endian(*value);
        }
    }
    Ok(values)
}

/// `copy_to_slice` then byteswap if the buffer format is non-native.
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
