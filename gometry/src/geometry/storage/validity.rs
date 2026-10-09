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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogicalRowIndex(usize);

impl LogicalRowIndex {
    pub(crate) const fn new(index: usize) -> Self {
        Self(index)
    }

    pub(crate) const fn get(self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PresentRowIndex(usize);

impl PresentRowIndex {
    pub(crate) const fn get(self) -> usize {
        self.0
    }
}

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

/// Validity publisher for one logical row stream.
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
        self.bytes.as_ref().is_none_or(|bytes| {
            bytes
                .get(row / 8)
                .expect("validity bitmap covers every admitted row")
                & (1 << (row % 8))
                != 0
        })
    }

    pub(crate) fn push(&mut self, present: bool) {
        let end = self.len.wrapping_add(1);
        if !present && self.bytes.is_none() {
            let mut bytes = Vec::with_capacity(self.capacity_hint.max(end).div_ceil(8));
            bytes.resize(end.div_ceil(8), u8::MAX);
            self.bytes = Some(bytes);
        } else if let Some(bytes) = &mut self.bytes
            && self.len.is_multiple_of(8)
        {
            bytes.push(0);
        }
        if let Some(bytes) = &mut self.bytes {
            let byte = bytes.last_mut().expect("current row has a validity byte");
            let bit = self.len % 8;
            if present {
                *byte |= 1 << bit;
            } else {
                *byte &= !(1 << bit);
            }
        }
        if present {
            self.valid_count = self.valid_count.wrapping_add(1);
        }
        self.len = end;
    }

    /// Append an all-present run without visiting one validity bit per row.
    ///
    /// A bitmap is allocated only when an earlier row was missing. Newly
    /// covered bytes are initialized to zero before the run is marked
    /// present; unused trailing bits are normalized by [`Self::finish`].
    pub(crate) fn extend_present(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        let end = self.len.wrapping_add(count);
        if let Some(bytes) = &mut self.bytes {
            let required = end.div_ceil(8);
            bytes.resize(required, 0);
            let start = self.len;
            let first = start / 8;
            let last = end.wrapping_sub(1) / 8;
            if first == last {
                let high = end % 8;
                let high_mask = if high == 0 {
                    u8::MAX
                } else {
                    (1_u8 << high).wrapping_sub(1)
                };
                *bytes
                    .get_mut(first)
                    .expect("validity bitmap covers the present run") |=
                    high_mask & (u8::MAX << (start % 8));
            } else {
                let row_bytes = bytes
                    .get_mut(first..=last)
                    .expect("validity bitmap covers the present run");
                let (first_byte, rest) = row_bytes
                    .split_first_mut()
                    .expect("present run has a first byte");
                let (last_byte, middle) =
                    rest.split_last_mut().expect("present run has a last byte");
                *first_byte |= u8::MAX << (start % 8);
                middle.fill(u8::MAX);
                let high = end % 8;
                *last_byte |= if high == 0 {
                    u8::MAX
                } else {
                    (1_u8 << high).wrapping_sub(1)
                };
            }
        }
        self.len = end;
        self.valid_count = self.valid_count.wrapping_add(count);
    }

    pub(crate) fn extend_missing(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        let start = self.len;
        let end = start.wrapping_add(count);
        let bytes = self.bytes.get_or_insert_with(|| {
            let mut bytes = Vec::with_capacity(self.capacity_hint.max(end).div_ceil(8));
            bytes.resize(start.div_ceil(8), u8::MAX);
            bytes
        });
        bytes.resize(end.div_ceil(8), 0);
        *bytes
            .get_mut(start / 8)
            .expect("validity bitmap covers the missing run") &=
            (1_u8 << (start % 8)).wrapping_sub(1);
        bytes
            .get_mut(start / 8..)
            .expect("validity bitmap covers the missing run")
            .iter_mut()
            .skip(1)
            .for_each(|byte| *byte = 0);
        self.len = end;
    }

    pub(crate) fn finish(self) -> Option<ValidityMask> {
        if self.valid_count == self.len {
            return None;
        }
        let mut bytes = self
            .bytes
            .expect("a missing row initializes the validity bitmap");
        if let Some(last) = bytes.last_mut() {
            *last &= trailing_mask(self.len);
        }
        Some(ValidityMask {
            bytes: SharedSlice::from_vec(bytes),
            bit_offset: 0,
            len: self.len,
            valid_count: self.valid_count,
        })
    }
}

impl ValidityMask {
    /// Build from the public/pickle convention (`true = missing`).
    pub(crate) fn from_vec(len: usize, missing: Vec<bool>) -> Option<Self> {
        assert_eq!(
            missing.len(),
            len,
            "missing mask length must match array length"
        );
        Self::from_missing_iter(len, missing)
    }

    pub(crate) fn from_missing_iter(
        len: usize,
        missing: impl IntoIterator<Item = bool>,
    ) -> Option<Self> {
        let mut rows = missing.into_iter().enumerate();
        let (first, _) = rows.find(|&(_, missing)| missing)?;
        let mut bytes = vec![u8::MAX; len.div_ceil(8)];
        *bytes
            .get_mut(first / 8)
            .expect("missing row is inside the declared validity length") &= !(1 << (first % 8));
        let mut missing_count = 1_usize;
        for (row, is_missing) in rows {
            if is_missing {
                *bytes
                    .get_mut(row / 8)
                    .expect("missing row is inside the declared validity length") &=
                    !(1 << (row % 8));
                missing_count = missing_count.wrapping_add(1);
            }
        }
        if let Some(last) = bytes.last_mut() {
            *last &= trailing_mask(len);
        }
        Some(Self {
            bytes: SharedSlice::from_vec(bytes),
            bit_offset: 0,
            len,
            valid_count: len.wrapping_sub(missing_count),
        })
    }

    /// Share an already bounded Arrow bitmap window.
    pub(crate) fn from_bitmap(
        bytes: SharedSlice<u8>,
        bit_offset: usize,
        len: usize,
    ) -> Option<Self> {
        let valid_count = count_valid(&bytes, bit_offset, len);
        (valid_count != len).then_some(Self {
            bytes,
            bit_offset,
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
            *bytes
                .get_mut(row / 8)
                .expect("missing row is inside the declared validity length") &= !(1 << (row % 8));
        }
        let valid_count = count_valid(&bytes, 0, len);
        Some(Self {
            bytes: SharedSlice::from_vec(bytes),
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
        bit(&self.bytes, self.bit_offset.wrapping_add(row))
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
    pub(crate) fn rank_for_rows(&self, rows: impl IntoIterator<Item = usize>) -> ValidityRank<'_> {
        let mut scalar_work = 0_usize;
        let mut directory_work = self.bytes.len();
        let mut row_count = 0_usize;
        for logical in rows {
            if self.is_valid(logical) {
                scalar_work = scalar_work.saturating_add(rank_scan_bytes(self.bit_offset, logical));
            }
            directory_work =
                directory_work.saturating_add(rank_block_scan_bytes(self.bit_offset, logical));
            row_count = row_count.saturating_add(1);
        }
        if row_count <= 1 || scalar_work <= directory_work {
            return ValidityRank {
                mask: self,
                blocks: None,
            };
        }
        ValidityRank {
            mask: self,
            blocks: Some(build_rank_blocks(self)),
        }
    }

    /// Build the operation-local directory used by full compact-point
    /// traversals. The directory is intentionally not retained on the mask:
    /// random scalar lookup and one-off gathers do not pay for it.
    pub(crate) fn full_rank(&self) -> ValidityRank<'_> {
        ValidityRank {
            mask: self,
            blocks: Some(build_rank_blocks(self)),
        }
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
        self.len.wrapping_sub(self.valid_count)
    }

    pub(crate) const fn present_count(&self) -> usize {
        self.valid_count
    }

    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "rows is a Range while self is a bitmap; each endpoint is intentionally checked against its own bound"
    )]
    pub(crate) fn present_in_range(&self, rows: std::ops::Range<usize>) -> usize {
        assert!(
            rows.start <= rows.end && rows.end <= self.len,
            "validity range must be within its logical rows"
        );
        count_valid(
            &self.bytes,
            self.bit_offset.wrapping_add(rows.start),
            rows.len(),
        )
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
        let bit_offset = self.bit_offset.wrapping_add(start);
        let valid_count = count_valid(&self.bytes, bit_offset, len);
        (valid_count != len).then(|| Self {
            bytes: self.bytes.clone(),
            bit_offset,
            len,
            valid_count,
        })
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
        self.resolve_present(LogicalRowIndex::new(logical))
            .map(PresentRowIndex::get)
    }

    pub(crate) fn resolve_present(&self, logical: LogicalRowIndex) -> Option<PresentRowIndex> {
        let logical = logical.get();
        if !self.mask.is_valid(logical) {
            return None;
        }
        Some(PresentRowIndex(self.rank(logical)))
    }

    pub(crate) fn rank(&self, logical: usize) -> usize {
        assert!(logical <= self.mask.len, "validity rank must be in bounds");
        let Some(blocks) = &self.blocks else {
            return self.mask.present_before(logical);
        };
        let block = logical / RANK_BLOCK_BITS;
        let block_start = block.wrapping_mul(RANK_BLOCK_BITS);
        blocks
            .get(block)
            .expect("rank directory covers every logical block")
            .wrapping_add(count_valid(
                &self.mask.bytes,
                self.mask.bit_offset.wrapping_add(block_start),
                logical.wrapping_sub(block_start),
            ))
    }
}

fn build_rank_blocks(mask: &ValidityMask) -> Vec<usize> {
    let block_count = mask.len.div_ceil(RANK_BLOCK_BITS);
    let mut blocks = Vec::with_capacity(block_count.wrapping_add(1));
    blocks.push(0);
    let mut present = 0_usize;
    for block in 0..block_count {
        let start = block.wrapping_mul(RANK_BLOCK_BITS);
        let len = mask.len.wrapping_sub(start).min(RANK_BLOCK_BITS);
        present = present.wrapping_add(count_valid(
            &mask.bytes,
            mask.bit_offset.wrapping_add(start),
            len,
        ));
        blocks.push(present);
    }
    blocks
}

const fn rank_scan_bytes(bit_offset: usize, logical: usize) -> usize {
    if logical == 0 {
        return 0;
    }
    let first = bit_offset / 8;
    let last = bit_offset.wrapping_add(logical).wrapping_sub(1) / 8;
    last.wrapping_sub(first).wrapping_add(1)
}

const fn rank_block_scan_bytes(bit_offset: usize, logical: usize) -> usize {
    let within = logical % RANK_BLOCK_BITS;
    if within == 0 {
        0
    } else {
        within.wrapping_add(bit_offset % 8).div_ceil(8)
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

#[derive(Clone)]
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
            (
                *mask
                    .bytes
                    .get(byte)
                    .expect("validity bitmap covers every nonempty window")
                    >> bit,
                8_u8.wrapping_sub(bit),
            )
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
            self.byte = self.byte.wrapping_add(1);
            self.bits = *self
                .bytes
                .get(self.byte)
                .expect("validity iterator remains inside its admitted bitmap window");
            self.bits_left = 8;
        }
        let missing = self.bits & 1 == 0;
        self.bits >>= 1;
        self.bits_left = self.bits_left.wrapping_sub(1);
        self.remaining = self.remaining.wrapping_sub(1);
        Some(missing)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for MissingIter<'_> {}

#[derive(Clone)]
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
        self.logical = self.logical.wrapping_add(1);
        let present = (!missing).then(|| {
            let present = self.present;
            self.present = self.present.wrapping_add(1);
            present
        });
        Some((logical, present))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.missing.size_hint()
    }
}

impl ExactSizeIterator for ValidityCursor<'_> {}

fn bit(bytes: &[u8], index: usize) -> bool {
    bytes
        .get(index / 8)
        .expect("validity bitmap covers the requested bit")
        & (1 << (index % 8))
        != 0
}

fn count_valid(bytes: &[u8], offset: usize, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let end = offset.wrapping_add(len);
    let first_byte = offset / 8;
    let last_byte = end.wrapping_sub(1) / 8;
    #[cfg(test)]
    COUNT_VALID_BYTES.with(|count| {
        count.set(
            count
                .get()
                .saturating_add(last_byte.saturating_sub(first_byte).saturating_add(1)),
        );
    });
    if first_byte == last_byte {
        let low = (offset % 8) as u32;
        let high = (end % 8) as u32;
        let high_mask = if high == 0 {
            u8::MAX
        } else {
            (1_u8 << high).wrapping_sub(1)
        };
        return (*bytes
            .get(first_byte)
            .expect("validity bitmap covers the requested byte")
            & high_mask
            & (u8::MAX << low))
            .count_ones() as usize;
    }
    let byte_window = bytes
        .get(first_byte..=last_byte)
        .expect("validity bitmap covers the requested byte range");
    let (first, remaining) = byte_window
        .split_first()
        .expect("validity byte range includes its first byte");
    let (last, middle) = remaining
        .split_last()
        .expect("multi-byte validity range includes its last byte");
    let mut count = (*first & (u8::MAX << (offset % 8))).count_ones() as usize;
    for byte in middle {
        count = count.wrapping_add(byte.count_ones() as usize);
    }
    let trailing = end % 8;
    count.wrapping_add(if trailing == 0 {
        last.count_ones() as usize
    } else {
        (last & ((1_u8 << trailing).wrapping_sub(1))).count_ones() as usize
    })
}

const fn trailing_mask(len: usize) -> u8 {
    let bits = len % 8;
    if bits == 0 {
        u8::MAX
    } else {
        (1_u8 << bits).wrapping_sub(1)
    }
}

/// Missing propagation is validity intersection (`present iff all present`).
pub(crate) fn union_many<'a>(
    masks: impl IntoIterator<Item = Option<&'a ValidityMask>>,
) -> Option<ValidityMask> {
    let mut masks = masks.into_iter().flatten().peekable();
    let first = masks.next()?;
    if masks.peek().is_none() {
        return Some(first.clone());
    }
    let mut bytes = vec![u8::MAX; first.len().div_ceil(8)];
    for mask in std::iter::once(first).chain(masks) {
        assert_eq!(
            first.len(),
            mask.len(),
            "validity union requires row alignment"
        );
        let source = mask
            .bytes
            .get(mask.bit_offset / 8..)
            .expect("validity bitmap covers its logical window");
        let shift = mask.bit_offset % 8;
        if shift == 0 {
            for (output, &input) in bytes.iter_mut().zip(source) {
                *output &= input;
            }
        } else {
            let zero = 0_u8;
            for (output, (&low, &high)) in bytes.iter_mut().zip(
                source
                    .iter()
                    .zip(source.iter().skip(1).chain(std::iter::once(&zero))),
            ) {
                *output &= (low >> shift) | (high << 8_usize.wrapping_sub(shift));
            }
        }
    }
    if let Some(last) = bytes.last_mut() {
        *last &= trailing_mask(first.len());
    }
    ValidityMask::from_bitmap(SharedSlice::from_vec(bytes), 0, first.len())
}

#[cfg(test)]
mod tests {
    #[test]
    fn validity_builder_publishes_dense_as_none_after_counting_rows() {
        use super::ValidityBuilder;

        let mut builder = ValidityBuilder::new();
        builder.push(true);
        builder.push(true);
        assert_eq!(builder.len(), 2);
        assert_eq!(builder.present_count(), 2);
        assert!(builder.finish().is_none());
    }

    #[test]
    fn validity_builder_keeps_bit_offset_zero_and_missing_polarity() {
        use super::ValidityBuilder;

        let mut builder = ValidityBuilder::new();
        for present in [false, true, false, true, true, false, true, false, true] {
            builder.push(present);
        }
        let mask = builder.finish().unwrap();
        assert_eq!(mask.iter().collect::<Vec<_>>(), [
            true, false, true, false, false, true, false, true, false
        ]);
        assert_eq!(mask.arrow_bitmap().1, 0);
    }

    #[test]
    fn validity_builder_clears_unused_trailing_bits_after_a_reserved_hint() {
        use super::ValidityBuilder;

        let mut builder = ValidityBuilder::with_capacity(64);
        builder.push(false);
        builder.push(true);
        let mask = builder.finish().unwrap();
        assert_eq!(mask.arrow_bitmap().0.as_ref(), &[0b10]);
    }

    #[test]
    fn validity_builder_reserves_the_first_missing_run_once() {
        for prefix in [0, 7, 8, 9, 511] {
            let mut builder = super::ValidityBuilder::new();
            builder.extend_present(prefix);
            let ((), allocations) =
                crate::test_support::allocation_count(|| builder.extend_missing(4096));
            assert_eq!(allocations, 1, "prefix {prefix}");
            let mask = builder.finish().expect("missing run");
            assert_eq!(mask.present_count(), prefix);
            assert_eq!(mask.missing_count(), 4096);
            assert!(mask.iter().take(prefix).all(|missing| !missing));
            assert!(mask.iter().skip(prefix).all(|missing| missing));
        }
    }

    #[test]
    fn validity_builder_bulk_runs_preserve_bits_and_counts() {
        use super::ValidityBuilder;

        for prefix in 0..16 {
            for missing in [0, 1, 7, 8, 9, 31] {
                let mut builder = ValidityBuilder::new();
                builder.extend_present(prefix);
                builder.extend_missing(missing);
                builder.extend_present(17);
                builder.extend_missing(3);
                builder.push(true);
                let mask = builder.finish().unwrap();
                let expected: Vec<_> = (0..prefix + missing + 21)
                    .map(|row| {
                        (prefix..prefix + missing).contains(&row)
                            || (prefix + missing + 17..prefix + missing + 20).contains(&row)
                    })
                    .collect();
                assert_eq!(mask.iter().collect::<Vec<_>>(), expected);
                assert_eq!(mask.present_count(), prefix + 18);
            }
        }
    }

    use super::{
        RANK_BLOCK_BYTES, ValidityMask, count_valid_bytes, reset_count_valid_bytes, union_many,
    };
    use crate::heap_size::HeapSize as _;

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
        assert!(std::ptr::eq(
            mask.arrow_bitmap().0.as_ptr(),
            nested.arrow_bitmap().0.as_ptr()
        ));

        let gathered =
            ValidityMask::from_missing_iter(5, [10, 0, 10, 1, 8].map(|row| mask.is_missing(row)))
                .unwrap();
        assert_eq!(gathered.iter().collect::<Vec<_>>(), [
            true, false, true, true, true
        ]);
        assert_eq!(gathered.arrow_bitmap().0.as_ref(), &[0b0000_0010]);
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
    fn operation_rank_directory_is_linear_for_unaligned_large_batches() {
        let len = 10_000;
        let mask = ValidityMask::from_sparse(len, &[len - 1]).unwrap();
        let rows: Vec<_> = (0..len).collect();

        reset_count_valid_bytes();
        let rank = mask.rank_for_rows(rows.iter().copied());
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
            let mask = ValidityMask::from_sparse(len, &[len - 1]).unwrap();
            let rows: Vec<_> = (0..len).collect();
            reset_count_valid_bytes();
            let rank = mask.rank_for_rows(rows.iter().copied());
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
        let source = ValidityMask::from_sparse(64, &[2, 9, 31, 62]).unwrap();
        let mask = source.window(7, 41).unwrap();
        let rows = [40, 0, 40, 1, 17, 39];
        let rank = mask.rank_for_rows(rows.iter().copied());
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
        .unwrap();
        let right = ValidityMask::from_vec(9, vec![
            false, false, true, false, false, false, false, false, false,
        ])
        .unwrap();
        let pair = union_many([Some(&left), Some(&right)]).unwrap();
        assert_eq!(pair.arrow_bitmap().0.as_ref(), &[0b1111_1001, 0]);
        assert_eq!(pair.iter().collect::<Vec<_>>(), [
            false, true, true, false, false, false, false, false, true
        ]);
        assert_eq!(union_many([Some(&left), None]), Some(left.clone()));
        assert_eq!(union_many([Some(&left), None, Some(&right)]), Some(pair));
    }

    #[test]
    fn union_handles_unaligned_windows_with_independent_missing_counts() {
        for left_offset in 0..16 {
            for right_offset in 0..16 {
                for len in [1, 7, 8, 9, 15, 16, 17, 65] {
                    let left = ValidityMask::from_missing_iter(
                        left_offset + len,
                        (0..left_offset + len)
                            .map(|row| row < left_offset || (row - left_offset) % 3 == 0),
                    )
                    .unwrap()
                    .window(left_offset, len)
                    .unwrap();
                    let right = ValidityMask::from_missing_iter(
                        right_offset + len,
                        (0..right_offset + len)
                            .map(|row| row >= right_offset && (row - right_offset) % 5 == 0),
                    )
                    .unwrap()
                    .window(right_offset, len)
                    .unwrap();
                    let union = union_many([Some(&left), None, Some(&right)]).unwrap();
                    let expected: Vec<_> =
                        (0..len).map(|row| row % 3 == 0 || row % 5 == 0).collect();
                    assert_eq!(union.iter().collect::<Vec<_>>(), expected);
                    assert_eq!(
                        union.missing_count(),
                        expected.iter().filter(|&&v| v).count()
                    );
                    assert_eq!(union.present_count(), len - union.missing_count());
                }
            }
        }
    }

    #[test]
    fn union_many_duplicate_masks_preserves_their_logical_bits() {
        let mask = ValidityMask::from_sparse(9, &[1, 7]).unwrap();
        let union = union_many([Some(&mask), Some(&mask)]).unwrap();
        assert_eq!(union, mask);
        assert_eq!(union.iter().collect::<Vec<_>>(), [
            false, true, false, false, false, false, false, true, false,
        ]);
        assert_eq!(union.missing_count(), 2);
        assert_eq!(union.present_count(), 7);
    }

    #[test]
    fn empty_and_all_present_inputs_produce_no_mask() {
        assert!(ValidityMask::from_vec(0, Vec::new()).is_none());
        assert!(ValidityMask::from_vec(5, vec![false; 5]).is_none());
        assert!(union_many([None, None]).is_none());
    }

    #[test]
    #[should_panic(expected = "validity union requires row alignment")]
    fn union_many_rejects_mismatched_lengths() {
        let left = ValidityMask::from_vec(1, vec![true]).unwrap();
        let right = ValidityMask::from_vec(2, vec![true, false]).unwrap();
        drop(union_many([Some(&left), Some(&right)]));
    }

    #[test]
    fn equality_compares_live_bits_for_a_window_and_packed_mask() {
        let window = ValidityMask::from_sparse(16, &[1, 4])
            .unwrap()
            .window(1, 5)
            .unwrap();
        let packed = ValidityMask::from_vec(5, vec![true, false, false, true, false]).unwrap();
        assert_eq!(window, packed);

        let different = ValidityMask::from_vec(5, vec![true, false, false, false, false]).unwrap();
        assert_ne!(window, different);
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
        assert!(std::ptr::eq(parent_bitmap.as_ptr(), window_bitmap.as_ptr()));

        assert_eq!(parent_bitmap.len(), len.div_ceil(8));
        let parent_retained = parent_bitmap.heap_bytes();
        assert_eq!(parent.heap_bytes(), parent_retained);
        assert_eq!(window.heap_bytes(), parent_retained);
        assert_eq!(nested.heap_bytes(), parent_retained);

        let gathered = ValidityMask::from_missing_iter(
            3,
            [2048, 2049, 2050].map(|row| parent.is_missing(row)),
        )
        .unwrap();
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
