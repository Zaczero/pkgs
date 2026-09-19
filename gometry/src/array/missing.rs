//! Shared row validity: one immutable Arrow-compatible bitmap for every
//! nullable geometry lane.
//!
//! A set bit means present. Arrays with no missing rows store `None`, so the
//! dense path allocates no validity bytes. Nontrivial masks may be logical
//! windows over shared immutable bytes.

use crate::geometry::SharedSlice;
use crate::heap_size::HeapSize;

const RANK_BLOCK_BITS: usize = 512;
#[cfg(test)]
const RANK_BLOCK_BYTES: usize = RANK_BLOCK_BITS / 8;

#[cfg(test)]
std::thread_local! {
    static COUNT_VALID_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Immutable row validity for a `GeometryArray` (`1 = present`, `0 = missing`).
#[derive(Clone, Debug)]
pub(crate) struct ValidityMask {
    bytes: SharedSlice<u8>,
    bit_offset: usize,
    len: usize,
    valid_count: usize,
}

/// Fallible validity publisher for one logical row stream.
///
/// The builder keeps the Arrow polarity (`1 = present`) while rows are
/// admitted, so nullable producers do not need a parallel missing-row index
/// or a second pass to construct the final bitmap.
pub(crate) struct ValidityBuilder {
    bytes: Option<Vec<u8>>,
    len: usize,
    valid_count: usize,
    capacity_hint: usize,
}

impl ValidityBuilder {
    pub(crate) const fn new() -> Self {
        Self {
            bytes: None,
            len: 0,
            valid_count: 0,
            capacity_hint: 0,
        }
    }

    pub(crate) fn with_capacity(len: usize) -> Self {
        Self {
            capacity_hint: len,
            ..Self::new()
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) const fn present_count(&self) -> usize {
        self.valid_count
    }

    pub(crate) fn is_present(&self, row: usize) -> bool {
        assert!(row < self.len, "validity row must be in bounds");
        self.bytes
            .as_ref()
            .is_none_or(|bytes| bytes[row / 8] & (1 << (row % 8)) != 0)
    }

    pub(crate) fn push(&mut self, present: bool) -> crate::error::Result<()> {
        if self.len == usize::MAX {
            return Err(crate::error::ResourceError::SizeOverflow.into());
        }
        if !present && self.bytes.is_none() {
            let byte_len = self
                .capacity_hint
                .max(
                    self.len
                        .checked_add(1)
                        .ok_or(crate::error::ResourceError::SizeOverflow)?,
                )
                .div_ceil(8);
            let mut bytes = Vec::new();
            crate::boundary::try_reserve_vec(&mut bytes, byte_len)?;
            bytes.resize((self.len + 1).div_ceil(8), u8::MAX);
            bytes[self.len / 8] &= !(1 << (self.len % 8));
            self.bytes = Some(bytes);
        } else if let Some(bytes) = &mut self.bytes
            && self.len.is_multiple_of(8)
        {
            crate::try_push_vec(bytes, 0)?;
        }
        if let Some(bytes) = &mut self.bytes {
            let byte = self.len / 8;
            let bit = self.len % 8;
            if present {
                bytes[byte] |= 1 << bit;
            } else {
                bytes[byte] &= !(1 << bit);
            }
        }
        if present {
            self.valid_count += 1;
        }
        self.len += 1;
        Ok(())
    }

    /// Append an all-present run without visiting one validity bit per row.
    ///
    /// A bitmap is allocated only when an earlier row was missing. Newly
    /// covered bytes are initialized to zero before the run is marked
    /// present; unused trailing bits are normalized by [`Self::finish`].
    pub(crate) fn extend_present(&mut self, count: usize) -> crate::error::Result<()> {
        if count == 0 {
            return Ok(());
        }
        let end = self
            .len
            .checked_add(count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let valid_count = self
            .valid_count
            .checked_add(count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if let Some(bytes) = &mut self.bytes {
            let required = end.div_ceil(8);
            if required > bytes.len() {
                let additional = required - bytes.len();
                crate::boundary::try_reserve_vec(bytes, additional)?;
                for _ in 0..additional {
                    crate::try_push_vec(bytes, 0)?;
                }
            }
            let start = self.len;
            let first = start / 8;
            let last = (end - 1) / 8;
            if first == last {
                let high = end % 8;
                let high_mask = if high == 0 {
                    u8::MAX
                } else {
                    (1_u8 << high) - 1
                };
                bytes[first] |= high_mask & (u8::MAX << (start % 8));
            } else {
                bytes[first] |= u8::MAX << (start % 8);
                for byte in &mut bytes[first + 1..last] {
                    *byte = u8::MAX;
                }
                let high = end % 8;
                bytes[last] |= if high == 0 {
                    u8::MAX
                } else {
                    (1_u8 << high) - 1
                };
            }
        }
        self.len = end;
        self.valid_count = valid_count;
        Ok(())
    }

    pub(crate) fn finish(self) -> crate::error::Result<Option<ValidityMask>> {
        if self.valid_count == self.len {
            return Ok(None);
        }
        let mut bytes = self
            .bytes
            .expect("a missing row initializes the validity bitmap");
        if let Some(last) = bytes.last_mut() {
            *last &= trailing_mask(self.len);
        }
        Ok(Some(ValidityMask {
            bytes: SharedSlice::try_from_vec(bytes)?,
            bit_offset: 0,
            len: self.len,
            valid_count: self.valid_count,
        }))
    }
}

impl ValidityMask {
    /// Build from the public/pickle convention (`true = missing`).
    #[expect(
        clippy::panic_in_result_fn,
        reason = "mask length is a caller invariant; the Result is the allocation path"
    )]
    pub(crate) fn from_vec(len: usize, missing: Vec<bool>) -> crate::error::Result<Option<Self>> {
        assert_eq!(
            missing.len(),
            len,
            "missing mask length must match array length"
        );
        Self::from_missing_iter(len, missing)
    }

    fn from_missing_iter(
        len: usize,
        missing: impl IntoIterator<Item = bool>,
    ) -> crate::error::Result<Option<Self>> {
        let mut bytes = Vec::new();
        crate::try_reserve_vec(&mut bytes, len.div_ceil(8))?;
        bytes.resize(len.div_ceil(8), 0);
        let mut valid_count = 0;
        let mut rows = 0;
        for (row, is_missing) in missing.into_iter().enumerate() {
            debug_assert!(row < len, "validity iterator exceeds declared length");
            if !is_missing {
                bytes[row / 8] |= 1 << (row % 8);
                valid_count += 1;
            }
            rows = row + 1;
        }
        debug_assert_eq!(
            rows, len,
            "validity iterator length must match declared length"
        );
        if valid_count == len {
            return Ok(None);
        }
        Ok(Some(Self {
            bytes: SharedSlice::try_from_vec(bytes)?,
            bit_offset: 0,
            len,
            valid_count,
        }))
    }

    #[expect(
        clippy::panic_in_result_fn,
        reason = "missing-row indices are a caller invariant; the Result is the allocation path"
    )]
    pub(crate) fn from_sparse(
        len: usize,
        missing_rows: &[usize],
    ) -> crate::error::Result<Option<Self>> {
        if missing_rows.is_empty() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        crate::try_reserve_vec(&mut bytes, len.div_ceil(8))?;
        bytes.resize(len.div_ceil(8), u8::MAX);
        if let Some(last) = bytes.last_mut() {
            *last &= trailing_mask(len);
        }
        for &row in missing_rows {
            assert!(row < len, "missing row must be in bounds");
            bytes[row / 8] &= !(1 << (row % 8));
        }
        let valid_count = count_valid(&bytes, 0, len);
        Ok(Some(Self {
            bytes: SharedSlice::try_from_vec(bytes)?,
            bit_offset: 0,
            len,
            valid_count,
        }))
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

    /// Resolve a batch of logical rows with the cheapest operation-local rank
    /// strategy for that batch. A short batch keeps the scalar prefix walk;
    /// larger or repeated-prefix batches build a 512-bit directory once and
    /// then scan at most one block for each query.
    pub(crate) fn rank_for_rows(&self, rows: &[usize]) -> crate::error::Result<ValidityRank<'_>> {
        let scalar_work = rows.iter().fold(0_usize, |work, &logical| {
            if self.is_valid(logical) {
                work.saturating_add(rank_scan_bytes(self.bit_offset, logical))
            } else {
                work
            }
        });
        let directory_work =
            self.bytes
                .len()
                .saturating_add(rows.iter().fold(0_usize, |work, &logical| {
                    work.saturating_add(rank_block_scan_bytes(self.bit_offset, logical))
                }));
        if rows.len() <= 1 || scalar_work <= directory_work {
            return Ok(ValidityRank {
                mask: self,
                blocks: None,
            });
        }
        Ok(ValidityRank {
            mask: self,
            blocks: Some(build_rank_blocks(self)?),
        })
    }

    /// Build the operation-local directory used by full compact-point
    /// traversals. The directory is intentionally not retained on the mask:
    /// random scalar lookup and one-off gathers do not pay for it.
    pub(crate) fn full_rank(&self) -> crate::error::Result<ValidityRank<'_>> {
        Ok(ValidityRank {
            mask: self,
            blocks: Some(build_rank_blocks(self)?),
        })
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
            bytes: self.bytes.clone(),
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
            // `is_valid` owns the bounds check.
            if self.is_valid(row) {
                bytes[out / 8] |= 1 << (out % 8);
                valid_count += 1;
            }
        }
        if valid_count == rows.len() {
            return Ok(None);
        }
        Ok(Some(Self {
            bytes: SharedSlice::try_from_vec(bytes)?,
            bit_offset: 0,
            len: rows.len(),
            valid_count,
        }))
    }

    /// Arrow-compatible owner and physical bit offset. The Arc keeps the bytes
    /// alive for C Data Interface consumers.
    pub(crate) fn arrow_bitmap(&self) -> (SharedSlice<u8>, usize) {
        (self.bytes.clone(), self.bit_offset)
    }
}

/// Operation-local rank resolver for one immutable validity mask.
///
/// `blocks` stores the number of present bits before each 512-bit block.
/// Its lifetime is limited to the operation that requested it, so ordinary
/// masks retain no index or cache state.
pub(crate) struct ValidityRank<'a> {
    mask: &'a ValidityMask,
    blocks: Option<Vec<usize>>,
}

impl ValidityRank<'_> {
    pub(crate) fn resolve(&self, logical: usize) -> Option<usize> {
        if !self.mask.is_valid(logical) {
            return None;
        }
        Some(self.rank(logical))
    }

    pub(crate) fn rank(&self, logical: usize) -> usize {
        assert!(logical <= self.mask.len, "validity rank must be in bounds");
        let Some(blocks) = &self.blocks else {
            return self.mask.present_before(logical);
        };
        let block = logical / RANK_BLOCK_BITS;
        let block_start = block * RANK_BLOCK_BITS;
        blocks[block]
            + count_valid(
                &self.mask.bytes,
                self.mask.bit_offset + block_start,
                logical - block_start,
            )
    }
}

fn build_rank_blocks(mask: &ValidityMask) -> crate::error::Result<Vec<usize>> {
    let block_count = mask.len.div_ceil(RANK_BLOCK_BITS);
    let mut blocks = Vec::new();
    crate::try_reserve_vec(&mut blocks, block_count + 1)?;
    blocks.push(0);
    let mut present = 0;
    for block in 0..block_count {
        let start = block * RANK_BLOCK_BITS;
        let len = (mask.len - start).min(RANK_BLOCK_BITS);
        present += count_valid(&mask.bytes, mask.bit_offset + start, len);
        crate::try_push_vec(&mut blocks, present)?;
    }
    Ok(blocks)
}

const fn rank_scan_bytes(bit_offset: usize, logical: usize) -> usize {
    if logical == 0 {
        return 0;
    }
    let first = bit_offset / 8;
    let last = (bit_offset + logical - 1) / 8;
    last - first + 1
}

const fn rank_block_scan_bytes(bit_offset: usize, logical: usize) -> usize {
    let within = logical % RANK_BLOCK_BITS;
    if within == 0 {
        0
    } else {
        (within + bit_offset % 8).div_ceil(8)
    }
}

#[cfg(test)]
pub(crate) fn reset_count_valid_bytes() {
    COUNT_VALID_BYTES.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn count_valid_bytes() -> usize {
    COUNT_VALID_BYTES.with(std::cell::Cell::get)
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
    bits: u8,
    bits_left: u8,
    remaining: usize,
}

impl<'a> MissingIter<'a> {
    fn new(mask: &'a ValidityMask) -> Self {
        let byte = mask.bit_offset / 8;
        let bit = (mask.bit_offset % 8) as u8;
        let (bits, bits_left) = if mask.len == 0 {
            (0, 0)
        } else {
            (mask.bytes[byte] >> bit, 8 - bit)
        };
        Self {
            bytes: &mask.bytes,
            byte,
            bits,
            bits_left,
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
        if self.bits_left == 0 {
            self.byte += 1;
            self.bits = self.bytes[self.byte];
            self.bits_left = 8;
        }
        let missing = self.bits & 1 == 0;
        self.bits >>= 1;
        self.bits_left -= 1;
        self.remaining -= 1;
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
    #[cfg(test)]
    COUNT_VALID_BYTES.with(|count| {
        count.set(count.get().saturating_add(last_byte - first_byte + 1));
    });
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
#[expect(
    clippy::panic_in_result_fn,
    reason = "paired masks are row-aligned by construction; the Result is the allocation path"
)]
pub(crate) fn union_pair(
    left: Option<&ValidityMask>,
    right: Option<&ValidityMask>,
) -> crate::error::Result<Option<ValidityMask>> {
    match (left, right) {
        (None, None) => Ok(None),
        (Some(mask), None) | (None, Some(mask)) => Ok(Some(mask.clone())),
        (Some(left), Some(right)) => {
            assert_eq!(
                left.len(),
                right.len(),
                "validity union requires row alignment"
            );
            ValidityMask::from_missing_iter(
                left.len(),
                left.iter()
                    .zip(right.iter())
                    .map(|(left, right)| left || right),
            )
        },
    }
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "unioned masks are row-aligned by construction; the Result is the allocation path"
)]
pub(crate) fn union_many<'a>(
    masks: impl IntoIterator<Item = Option<&'a ValidityMask>>,
) -> crate::error::Result<Option<ValidityMask>> {
    let mut collected = Vec::new();
    for mask in masks.into_iter().flatten() {
        crate::try_push_vec(&mut collected, mask)?;
    }
    let masks = collected;
    let Some(first) = masks.first() else {
        return Ok(None);
    };
    for mask in &masks[1..] {
        assert_eq!(
            first.len(),
            mask.len(),
            "validity union requires row alignment"
        );
    }
    ValidityMask::from_missing_iter(
        first.len(),
        (0..first.len()).map(|row| masks.iter().any(|mask| mask.is_missing(row))),
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn validity_builder_publishes_dense_as_none_after_counting_rows() {
        use super::ValidityBuilder;

        let mut builder = ValidityBuilder::new();
        builder.push(true).unwrap();
        builder.push(true).unwrap();
        assert_eq!(builder.len(), 2);
        assert_eq!(builder.present_count(), 2);
        assert!(builder.finish().unwrap().is_none());
    }

    #[test]
    fn validity_builder_keeps_bit_offset_zero_and_missing_polarity() {
        use super::ValidityBuilder;

        let mut builder = ValidityBuilder::new();
        for present in [false, true, false, true, true, false, true, false, true] {
            builder.push(present).unwrap();
        }
        let mask = builder.finish().unwrap().unwrap();
        assert_eq!(mask.iter().collect::<Vec<_>>(), [
            true, false, true, false, false, true, false, true, false
        ]);
        assert_eq!(mask.arrow_bitmap().1, 0);
    }

    #[test]
    fn validity_builder_clears_unused_trailing_bits_after_a_reserved_hint() {
        use super::ValidityBuilder;

        let mut builder = ValidityBuilder::with_capacity(64);
        builder.push(false).unwrap();
        builder.push(true).unwrap();
        let mask = builder.finish().unwrap().unwrap();
        assert_eq!(mask.arrow_bitmap().0.as_ref(), &[0b10]);
    }

    #[test]
    fn validity_builder_bulk_present_run_matches_scalar_bits() {
        use super::ValidityBuilder;

        let mut builder = ValidityBuilder::new();
        builder.push(false).unwrap();
        builder.extend_present(17).unwrap();
        builder.push(false).unwrap();
        builder.extend_present(3).unwrap();
        let mask = builder.finish().unwrap().unwrap();
        let mut expected = vec![true];
        expected.extend([false; 17]);
        expected.push(true);
        expected.extend([false; 3]);
        assert_eq!(mask.iter().collect::<Vec<_>>(), expected);
        assert_eq!(mask.present_count(), 20);
    }

    #[test]
    fn mask_publication_failure_preserves_source_and_allows_retry() {
        use super::ValidityMask;
        use crate::test_support::{allocation_count, fail_allocations_after};

        let source = ValidityMask::from_sparse(5, &[1, 4]).unwrap().unwrap();
        let rows = [4, 0, 4, 1];
        let (result, allocations) = allocation_count(|| source.try_gather(&rows));
        assert_eq!(result.unwrap().unwrap().iter().collect::<Vec<_>>(), [
            true, false, true, true
        ]);
        assert!(allocations > 0);
        for successful in 0..allocations {
            let error =
                fail_allocations_after(successful, || source.try_gather(&rows)).unwrap_err();
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::Allocation)
            ));
            assert_eq!(source.iter().collect::<Vec<_>>(), [
                false, true, false, false, true
            ]);
            let retry = source.try_gather(&rows).unwrap().unwrap();
            assert_eq!(retry.iter().collect::<Vec<_>>(), [true, false, true, true]);
        }
        let window = fail_allocations_after(0, || source.window(1, 3)).unwrap();
        assert_eq!(window.iter().collect::<Vec<_>>(), [true, false, false]);
    }

    use super::{
        RANK_BLOCK_BYTES, ValidityMask, count_valid_bytes, reset_count_valid_bytes, union_many,
        union_pair,
    };
    use crate::heap_size::HeapSize as _;

    #[test]
    fn polarity_windows_gather_concat_and_trailing_bits() {
        let mask = ValidityMask::from_vec(11, vec![
            false, true, false, true, false, false, true, false, true, false, true,
        ])
        .unwrap()
        .unwrap();
        assert_eq!(mask.arrow_bitmap().0.as_ref(), &[0b1011_0101, 0b0000_0010]);
        let window = mask.window(3, 7).unwrap();
        let nested = window.window(2, 4).unwrap();
        assert_eq!(nested.iter().collect::<Vec<_>>(), [
            false, true, false, true
        ]);
        assert!(std::ptr::eq(
            mask.arrow_bitmap().0.as_ptr(),
            nested.arrow_bitmap().0.as_ptr()
        ));

        let gathered = mask.try_gather(&[10, 0, 10, 1, 8]).unwrap().unwrap();
        assert_eq!(gathered.iter().collect::<Vec<_>>(), [
            true, false, true, true, true
        ]);
        assert_eq!(gathered.arrow_bitmap().0.as_ref(), &[0b0000_0010]);
    }

    #[test]
    fn all_present_is_none_and_all_missing_is_packed() {
        assert!(ValidityMask::from_vec(9, vec![false; 9]).unwrap().is_none());
        let missing = ValidityMask::from_vec(9, vec![true; 9]).unwrap().unwrap();
        assert_eq!(missing.arrow_bitmap().0.as_ref(), &[0, 0]);
        assert_eq!(missing.missing_count(), 9);
    }

    #[test]
    fn cursor_and_random_rank_agree_for_every_position() {
        let mask = ValidityMask::from_vec(11, vec![
            true, false, true, false, false, true, false, true, false, true, true,
        ])
        .unwrap()
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
    fn operation_rank_directory_is_linear_for_unaligned_large_batches() {
        let len = 10_000;
        let mask = ValidityMask::from_sparse(len, &[len - 1]).unwrap().unwrap();
        let rows: Vec<_> = (0..len).collect();

        reset_count_valid_bytes();
        let rank = mask.rank_for_rows(&rows).unwrap();
        let resolved = rows
            .iter()
            .map(|&row| rank.resolve(row))
            .collect::<Vec<_>>();
        let directory_bytes = count_valid_bytes();

        reset_count_valid_bytes();
        let scalar = rows
            .iter()
            .map(|&row| mask.is_valid(row).then(|| mask.present_before(row)))
            .collect::<Vec<_>>();
        let scalar_bytes = count_valid_bytes();

        assert_eq!(resolved, scalar);
        assert!(directory_bytes < scalar_bytes);
        assert!(
            directory_bytes <= mask.bytes.len() + rows.len() * RANK_BLOCK_BYTES,
            "directory visited {directory_bytes} bytes for {} rows",
            rows.len()
        );
    }

    #[test]
    fn operation_rank_directory_scales_linearly_across_batch_sizes() {
        for len in [10_000, 100_000, 1_000_000] {
            let mask = ValidityMask::from_sparse(len, &[len - 1]).unwrap().unwrap();
            let rows: Vec<_> = (0..len).collect();
            reset_count_valid_bytes();
            let rank = mask.rank_for_rows(&rows).unwrap();
            for &row in &rows {
                let _ = rank.resolve(row);
            }
            let bytes = count_valid_bytes();
            eprintln!("rank_directory len={len} count_valid_bytes={bytes}");
            assert!(
                bytes <= mask.bytes.len() + len * RANK_BLOCK_BYTES,
                "directory visited {bytes} bytes for {len} rows"
            );
        }
    }

    #[test]
    fn operation_rank_directory_handles_bit_seven_windows_and_duplicates() {
        let source = ValidityMask::from_sparse(64, &[2, 9, 31, 62])
            .unwrap()
            .unwrap();
        let mask = source.window(7, 41).unwrap();
        let rows = [40, 0, 40, 1, 17, 39];
        let rank = mask.rank_for_rows(&rows).unwrap();
        let expected = rows
            .iter()
            .map(|&row| mask.is_valid(row).then(|| mask.present_before(row)))
            .collect::<Vec<_>>();
        let actual = rows
            .iter()
            .map(|&row| rank.resolve(row))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn validity_intersection_propagates_missing_without_flipping_polarity() {
        let left = ValidityMask::from_vec(9, vec![
            false, true, false, false, false, false, false, false, true,
        ])
        .unwrap()
        .unwrap();
        let right = ValidityMask::from_vec(9, vec![
            false, false, true, false, false, false, false, false, false,
        ])
        .unwrap()
        .unwrap();
        let pair = union_pair(Some(&left), Some(&right)).unwrap().unwrap();
        assert_eq!(pair.arrow_bitmap().0.as_ref(), &[0b1111_1001, 0]);
        assert_eq!(pair.iter().collect::<Vec<_>>(), [
            false, true, true, false, false, false, false, false, true
        ]);
        assert_eq!(union_pair(Some(&left), None).unwrap(), Some(left.clone()));
        assert_eq!(
            union_many([Some(&left), None, Some(&right)]).unwrap(),
            Some(pair)
        );
    }

    #[test]
    fn union_handles_unaligned_windows_with_independent_missing_counts() {
        let offset_one = ValidityMask::from_sparse(16, &[1, 4])
            .unwrap()
            .unwrap()
            .window(1, 5)
            .unwrap();
        let offset_seven = ValidityMask::from_sparse(16, &[7, 9, 11])
            .unwrap()
            .unwrap()
            .window(7, 5)
            .unwrap();
        let union = union_pair(Some(&offset_one), Some(&offset_seven))
            .unwrap()
            .unwrap();
        assert_eq!(union.iter().collect::<Vec<_>>(), [
            true, false, true, true, true
        ]);
        assert_eq!(union.missing_count(), 4);
        assert_eq!(union.present_count(), 1);

        let offset_nine = ValidityMask::from_sparse(16, &[9, 12])
            .unwrap()
            .unwrap()
            .window(9, 5)
            .unwrap();
        let other = ValidityMask::from_vec(5, vec![false, false, true, false, false])
            .unwrap()
            .unwrap();
        let union = union_pair(Some(&offset_nine), Some(&other))
            .unwrap()
            .unwrap();
        assert_eq!(union.iter().collect::<Vec<_>>(), [
            true, false, true, true, false
        ]);
        assert_eq!(union.missing_count(), 3);
        assert_eq!(union.present_count(), 2);
    }

    #[test]
    fn union_many_duplicate_masks_preserves_their_logical_bits() {
        let mask = ValidityMask::from_sparse(9, &[1, 7]).unwrap().unwrap();
        let union = union_many([Some(&mask), Some(&mask)]).unwrap().unwrap();
        assert_eq!(union, mask);
        assert_eq!(union.iter().collect::<Vec<_>>(), [
            false, true, false, false, false, false, false, true, false,
        ]);
        assert_eq!(union.missing_count(), 2);
        assert_eq!(union.present_count(), 7);
    }

    #[test]
    fn empty_and_all_present_inputs_produce_no_mask() {
        assert!(ValidityMask::from_vec(0, Vec::new()).unwrap().is_none());
        assert!(ValidityMask::from_vec(5, vec![false; 5]).unwrap().is_none());
        assert!(union_pair(None, None).unwrap().is_none());
        assert!(union_many([None, None]).unwrap().is_none());
    }

    #[test]
    #[should_panic(expected = "validity union requires row alignment")]
    fn union_pair_rejects_mismatched_lengths() {
        let left = ValidityMask::from_vec(1, vec![true]).unwrap().unwrap();
        let right = ValidityMask::from_vec(2, vec![true, false])
            .unwrap()
            .unwrap();
        let _ = union_pair(Some(&left), Some(&right)).unwrap();
    }

    #[test]
    #[should_panic(expected = "validity union requires row alignment")]
    fn union_many_rejects_mismatched_lengths() {
        let left = ValidityMask::from_vec(1, vec![true]).unwrap().unwrap();
        let right = ValidityMask::from_vec(2, vec![true, false])
            .unwrap()
            .unwrap();
        let _ = union_many([Some(&left), Some(&right)]).unwrap();
    }

    #[test]
    fn equality_compares_live_bits_for_a_window_and_packed_mask() {
        let window = ValidityMask::from_sparse(16, &[1, 4])
            .unwrap()
            .unwrap()
            .window(1, 5)
            .unwrap();
        let packed = ValidityMask::from_vec(5, vec![true, false, false, true, false])
            .unwrap()
            .unwrap();
        assert_eq!(window, packed);

        let different = ValidityMask::from_vec(5, vec![true, false, false, false, false])
            .unwrap()
            .unwrap();
        assert_ne!(window, different);
    }

    #[test]
    fn retained_size_charges_shared_parent_but_gather_charges_exact_owner() {
        let len = 4097;
        let mut missing = vec![false; len];
        missing[2048] = true;
        missing[2050] = true;
        let parent = ValidityMask::from_vec(len, missing).unwrap().unwrap();
        let window = parent.window(2048, 3).unwrap();
        let nested = window.window(0, 2).unwrap();
        let (parent_bitmap, _) = parent.arrow_bitmap();
        let (window_bitmap, _) = window.arrow_bitmap();
        assert!(std::ptr::eq(parent_bitmap.as_ptr(), window_bitmap.as_ptr()));

        assert_eq!(parent_bitmap.len(), len.div_ceil(8));
        let parent_retained = parent_bitmap.heap_bytes();
        assert_eq!(parent.heap_bytes(), parent_retained);
        assert_eq!(window.heap_bytes(), parent_retained);
        assert_eq!(nested.heap_bytes(), parent_retained);

        let gathered = parent.try_gather(&[2048, 2049, 2050]).unwrap().unwrap();
        let (gathered_bitmap, _) = gathered.arrow_bitmap();
        assert!(!std::ptr::eq(
            parent_bitmap.as_ptr(),
            gathered_bitmap.as_ptr()
        ));
        assert_eq!(gathered_bitmap.len(), 1);
        assert_eq!(gathered.heap_bytes(), gathered_bitmap.heap_bytes());
        assert_eq!(Option::<ValidityMask>::None.heap_bytes(), 0);
    }
}
