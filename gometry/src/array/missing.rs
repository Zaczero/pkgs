//! Shared row validity: one immutable Arrow-compatible bitmap for every
//! nullable geometry lane.
//!
//! A set bit means present. Arrays with no missing rows store `None`, so the
//! dense path allocates no validity bytes. Nontrivial masks may be logical
//! windows over shared immutable bytes.

use std::sync::Arc;

use crate::heap_size::HeapSize;

/// Immutable row validity for a `GeometryArray` (`1 = present`, `0 = missing`).
#[derive(Clone, Debug)]
pub(crate) struct ValidityMask {
    bytes: Arc<[u8]>,
    bit_offset: usize,
    len: usize,
    valid_count: usize,
}

impl ValidityMask {
    /// Build from the public/pickle convention (`true = missing`).
    pub(crate) fn from_vec(len: usize, missing: Vec<bool>) -> Option<Self> {
        assert_eq!(
            missing.len(),
            len,
            "missing mask length must match array length"
        );
        let mut bytes = vec![0_u8; len.div_ceil(8)];
        let mut valid_count = 0;
        for (row, is_missing) in missing.into_iter().enumerate() {
            if !is_missing {
                bytes[row / 8] |= 1 << (row % 8);
                valid_count += 1;
            }
        }
        (valid_count != len).then(|| Self {
            bytes: bytes.into(),
            bit_offset: 0,
            len,
            valid_count,
        })
    }

    pub(crate) fn from_sparse(len: usize, missing_rows: &[usize]) -> Option<Self> {
        if missing_rows.is_empty() {
            return None;
        }
        let mut bytes = vec![u8::MAX; len.div_ceil(8)];
        if let Some(last) = bytes.last_mut() {
            *last &= trailing_mask(len);
        }
        for &row in missing_rows {
            assert!(row < len, "missing row must be in bounds");
            bytes[row / 8] &= !(1 << (row % 8));
        }
        let valid_count = count_valid(&bytes, 0, len);
        Some(Self {
            bytes: bytes.into(),
            bit_offset: 0,
            len,
            valid_count,
        })
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) const fn any(&self) -> bool {
        self.valid_count != self.len
    }

    pub(crate) fn is_valid(&self, row: usize) -> bool {
        assert!(row < self.len, "validity row must be in bounds");
        bit(&self.bytes, self.bit_offset + row)
    }

    pub(crate) fn is_missing(&self, row: usize) -> bool {
        !self.is_valid(row)
    }

    /// Number of present rows before `logical`. Random scalar access uses this
    /// checked byte-popcount path; sequential consumers should use `cursor`.
    pub(crate) fn present_before(&self, logical: usize) -> usize {
        assert!(logical <= self.len, "validity rank must be in bounds");
        count_valid(&self.bytes, self.bit_offset, logical)
    }

    pub(crate) fn cursor(&self) -> ValidityCursor<'_> {
        ValidityCursor {
            missing: self.iter(),
            logical: 0,
            present: 0,
        }
    }

    /// Sequential missingness cursor. It advances within one byte before
    /// loading the next, avoiding division in row kernels.
    pub(crate) fn iter(&self) -> MissingIter<'_> {
        MissingIter::new(self)
    }

    pub(crate) const fn missing_count(&self) -> usize {
        self.len - self.valid_count
    }

    pub(crate) const fn present_count(&self) -> usize {
        self.valid_count
    }

    /// Complete heap retained by this bitmap owner. Windows charge the shared
    /// parent allocation in full, matching the repository's Arc policy.
    pub(crate) fn retained_bytes(&self) -> usize {
        self.bytes.heap_bytes()
    }

    /// Logical subwindow. A non-all-valid result shares the same Arc bytes.
    pub(crate) fn window(&self, start: usize, len: usize) -> Option<Self> {
        let end = start
            .checked_add(len)
            .expect("validity window end overflowed");
        assert!(end <= self.len, "validity window must be in bounds");
        let bit_offset = self.bit_offset + start;
        let valid_count = count_valid(&self.bytes, bit_offset, len);
        (valid_count != len).then(|| Self {
            bytes: Arc::clone(&self.bytes),
            bit_offset,
            len,
            valid_count,
        })
    }

    /// Gather in exact row order, including reversed and duplicate rows.
    pub(crate) fn try_gather(&self, rows: &[usize]) -> crate::error::Result<Option<Self>> {
        let mut bytes = Vec::new();
        crate::try_reserve_vec(&mut bytes, rows.len().div_ceil(8))?;
        bytes.resize(rows.len().div_ceil(8), 0);
        let mut valid_count = 0;
        for (out, &row) in rows.iter().enumerate() {
            assert!(row < self.len, "validity gather row must be in bounds");
            if self.is_valid(row) {
                bytes[out / 8] |= 1 << (out % 8);
                valid_count += 1;
            }
        }
        Ok((valid_count != rows.len()).then(|| Self {
            bytes: bytes.into(),
            bit_offset: 0,
            len: rows.len(),
            valid_count,
        }))
    }

    /// Concatenate optional validity lanes. `None` contributes all-present bits.
    pub(crate) fn try_concat<'a>(
        parts: impl IntoIterator<Item = (usize, Option<&'a Self>)>,
    ) -> crate::error::Result<Option<Self>> {
        let parts = parts.into_iter();
        let mut collected = Vec::new();
        crate::try_reserve_vec(&mut collected, parts.size_hint().0)?;
        for part in parts {
            crate::try_push_vec(&mut collected, part)?;
        }
        let parts = collected;
        let len = parts.iter().try_fold(0_usize, |total, (len, mask)| {
            if let Some(mask) = mask {
                assert_eq!(*len, mask.len, "validity concat length must match mask");
            }
            total
                .checked_add(*len)
                .ok_or(crate::error::ResourceError::SizeOverflow)
        })?;
        let mut bytes = Vec::new();
        crate::try_reserve_vec(&mut bytes, len.div_ceil(8))?;
        bytes.resize(len.div_ceil(8), 0);
        let mut out = 0;
        let mut valid_count = 0;
        for (part_len, mask) in parts {
            for row in 0..part_len {
                let valid = mask.is_none_or(|mask| mask.is_valid(row));
                if valid {
                    bytes[out / 8] |= 1 << (out % 8);
                    valid_count += 1;
                }
                out += 1;
            }
        }
        Ok((valid_count != len).then(|| Self {
            bytes: bytes.into(),
            bit_offset: 0,
            len,
            valid_count,
        }))
    }

    /// Arrow-compatible owner and physical bit offset. The Arc keeps the bytes
    /// alive for C Data Interface consumers.
    pub(crate) fn arrow_bitmap(&self) -> (Arc<[u8]>, usize) {
        (Arc::clone(&self.bytes), self.bit_offset)
    }

    /// Current logical pickle/public mask (`0 = present`, `1 = missing`).
    pub(crate) fn to_missing_bytes(&self) -> Vec<u8> {
        self.iter().map(u8::from).collect()
    }
}

impl PartialEq for ValidityMask {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}

impl Eq for ValidityMask {}

impl HeapSize for ValidityMask {
    fn heap_bytes(&self) -> usize {
        self.retained_bytes()
    }
}

pub(crate) struct MissingIter<'a> {
    bytes: &'a [u8],
    byte: usize,
    bit: u8,
    remaining: usize,
}

impl<'a> MissingIter<'a> {
    fn new(mask: &'a ValidityMask) -> Self {
        Self {
            bytes: &mask.bytes,
            byte: mask.bit_offset / 8,
            bit: (mask.bit_offset % 8) as u8,
            remaining: mask.len,
        }
    }
}

impl Iterator for MissingIter<'_> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let missing = self.bytes[self.byte] & (1 << self.bit) == 0;
        self.remaining -= 1;
        self.bit += 1;
        if self.bit == 8 {
            self.bit = 0;
            self.byte += 1;
        }
        Some(missing)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for MissingIter<'_> {}

pub(crate) struct ValidityCursor<'a> {
    missing: MissingIter<'a>,
    logical: usize,
    present: usize,
}

impl Iterator for ValidityCursor<'_> {
    type Item = (usize, Option<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let missing = self.missing.next()?;
        let logical = self.logical;
        self.logical += 1;
        let present = (!missing).then(|| {
            let present = self.present;
            self.present += 1;
            present
        });
        Some((logical, present))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.missing.size_hint()
    }
}

impl ExactSizeIterator for ValidityCursor<'_> {}

const fn bit(bytes: &[u8], index: usize) -> bool {
    bytes[index / 8] & (1 << (index % 8)) != 0
}

fn count_valid(bytes: &[u8], offset: usize, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let end = offset + len;
    let first_byte = offset / 8;
    let last_byte = (end - 1) / 8;
    if first_byte == last_byte {
        let low = (offset % 8) as u32;
        let high = (end % 8) as u32;
        let high_mask = if high == 0 {
            u8::MAX
        } else {
            (1_u8 << high) - 1
        };
        return (bytes[first_byte] & high_mask & (u8::MAX << low)).count_ones() as usize;
    }
    let mut count = (bytes[first_byte] & (u8::MAX << (offset % 8))).count_ones() as usize;
    count += bytes[first_byte + 1..last_byte]
        .iter()
        .map(|byte| byte.count_ones() as usize)
        .sum::<usize>();
    let trailing = end % 8;
    count
        + if trailing == 0 {
            bytes[last_byte].count_ones() as usize
        } else {
            (bytes[last_byte] & ((1_u8 << trailing) - 1)).count_ones() as usize
        }
}

const fn trailing_mask(len: usize) -> u8 {
    let bits = len % 8;
    if bits == 0 {
        u8::MAX
    } else {
        (1_u8 << bits) - 1
    }
}

/// Missing propagation is validity intersection (`present iff both present`).
pub(crate) fn union_pair(
    left: Option<&ValidityMask>,
    right: Option<&ValidityMask>,
) -> Option<ValidityMask> {
    match (left, right) {
        (None, None) => None,
        (Some(mask), None) | (None, Some(mask)) => Some(mask.clone()),
        (Some(left), Some(right)) => {
            assert_eq!(
                left.len(),
                right.len(),
                "validity union requires row alignment"
            );
            ValidityMask::from_vec(
                left.len(),
                left.iter()
                    .zip(right.iter())
                    .map(|(left, right)| left || right)
                    .collect(),
            )
        },
    }
}

pub(crate) fn union_many<'a>(
    masks: impl IntoIterator<Item = Option<&'a ValidityMask>>,
) -> Option<ValidityMask> {
    let masks: Vec<_> = masks.into_iter().flatten().collect();
    let first = masks.first()?;
    for mask in &masks[1..] {
        assert_eq!(
            first.len(),
            mask.len(),
            "validity union requires row alignment"
        );
    }
    ValidityMask::from_vec(
        first.len(),
        (0..first.len())
            .map(|row| masks.iter().any(|mask| mask.is_missing(row)))
            .collect(),
    )
}

pub(crate) fn is_missing_row(mask: Option<&ValidityMask>, row: usize) -> bool {
    mask.is_some_and(|mask| mask.is_missing(row))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ValidityMask, union_many, union_pair};
    use crate::heap_size::HeapSize;

    #[test]
    fn polarity_windows_gather_concat_and_trailing_bits() {
        let mask = ValidityMask::from_vec(11, vec![
            false, true, false, true, false, false, true, false, true, false, true,
        ])
        .unwrap();
        assert_eq!(mask.arrow_bitmap().0.as_ref(), &[0b1011_0101, 0b0000_0010]);
        let window = mask.window(3, 7).unwrap();
        let nested = window.window(2, 4).unwrap();
        assert_eq!(nested.iter().collect::<Vec<_>>(), [
            false, true, false, true
        ]);
        assert!(Arc::ptr_eq(
            &mask.arrow_bitmap().0,
            &nested.arrow_bitmap().0
        ));

        let gathered = mask.try_gather(&[10, 0, 10, 1, 8]).unwrap().unwrap();
        assert_eq!(gathered.iter().collect::<Vec<_>>(), [
            true, false, true, true, true
        ]);
        assert_eq!(gathered.arrow_bitmap().0.as_ref(), &[0b0000_0010]);

        let joined = ValidityMask::try_concat([(2, None), (7, Some(&window))])
            .unwrap()
            .unwrap();
        assert_eq!(joined.iter().collect::<Vec<_>>(), [
            false, false, true, false, false, true, false, true, false
        ]);
        assert_eq!(joined.arrow_bitmap().0[1] & 0b1111_1110, 0);
    }

    #[test]
    fn all_present_is_none_and_all_missing_is_packed() {
        assert!(ValidityMask::from_vec(9, vec![false; 9]).is_none());
        let missing = ValidityMask::from_vec(9, vec![true; 9]).unwrap();
        assert_eq!(missing.arrow_bitmap().0.as_ref(), &[0, 0]);
        assert_eq!(missing.missing_count(), 9);
    }

    #[test]
    fn cursor_and_random_rank_agree_for_every_position() {
        let mask = ValidityMask::from_vec(11, vec![
            true, false, true, false, false, true, false, true, false, true, true,
        ])
        .unwrap();
        let rows: Vec<_> = mask.cursor().collect();
        assert_eq!(rows.len(), 11);
        for (logical, present) in rows {
            assert_eq!(
                present,
                mask.is_valid(logical).then(|| mask.present_before(logical))
            );
        }
        assert_eq!(mask.present_before(mask.len()), mask.present_count());
    }

    #[test]
    fn validity_intersection_propagates_missing_without_flipping_polarity() {
        let left = ValidityMask::from_vec(9, vec![
            false, true, false, false, false, false, false, false, true,
        ])
        .unwrap();
        let right = ValidityMask::from_vec(9, vec![
            false, false, true, false, false, false, false, false, false,
        ])
        .unwrap();
        let pair = union_pair(Some(&left), Some(&right)).unwrap();
        assert_eq!(pair.arrow_bitmap().0.as_ref(), &[0b1111_1001, 0]);
        assert_eq!(pair.iter().collect::<Vec<_>>(), [
            false, true, true, false, false, false, false, false, true
        ]);
        assert_eq!(union_pair(Some(&left), None), Some(left.clone()));
        assert_eq!(union_many([Some(&left), None, Some(&right)]), Some(pair));
    }

    #[test]
    fn retained_size_charges_shared_parent_but_gather_charges_exact_owner() {
        let len = 4097;
        let mut missing = vec![false; len];
        missing[2048] = true;
        missing[2050] = true;
        let parent = ValidityMask::from_vec(len, missing).unwrap();
        let window = parent.window(2048, 3).unwrap();
        let nested = window.window(0, 2).unwrap();
        let (parent_bitmap, _) = parent.arrow_bitmap();
        let (window_bitmap, _) = window.arrow_bitmap();
        assert!(Arc::ptr_eq(&parent_bitmap, &window_bitmap));

        assert_eq!(parent_bitmap.len(), len.div_ceil(8));
        let parent_retained = parent_bitmap.heap_bytes();
        assert_eq!(parent.heap_bytes(), parent_retained);
        assert_eq!(window.heap_bytes(), parent_retained);
        assert_eq!(nested.heap_bytes(), parent_retained);

        let gathered = parent.try_gather(&[2048, 2049, 2050]).unwrap().unwrap();
        let (gathered_bitmap, _) = gathered.arrow_bitmap();
        assert!(!Arc::ptr_eq(&parent_bitmap, &gathered_bitmap));
        assert_eq!(gathered_bitmap.len(), 1);
        assert_eq!(gathered.heap_bytes(), gathered_bitmap.heap_bytes());
        assert_eq!(Option::<ValidityMask>::None.heap_bytes(), 0);
    }
}
