#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Grid-kind registry and id coercion for `CellArray`: which scalar cell
//! type an array stores, per-grid id validation/round-tripping, and the
//! Python-input -> `Vec<u64>` collectors the constructor uses.

use h3o::CellIndex;
use numpy::{PyReadonlyArrayDyn, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyInt, PyString, PyType};

use super::geohash::PyGeohashCell;
use super::h3::PyH3Cell;
use super::s2::PyS2Cell;
use super::tiles::PyTile;
use crate::grid::geohash::{GEOHASH_MAX_PRECISION, Geohash};
use crate::grid::s2::cellid::CellId;
use crate::grid::tile::Tile;
use crate::py::errors::{GeometryError, ParseFormat, parse_error};

/// Which scalar cell type a `CellArray` stores.
#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum GridKind {
    H3Cell,
    S2Cell,
    Tile,
    GeohashCell,
}

struct GridMeta {
    kind: GridKind,
    token: &'static str,
    item_name: &'static str,
    label: &'static str,
    class_name: &'static str,
    format: ParseFormat,
}

const GRID_META: [GridMeta; 4] = [
    GridMeta {
        kind: GridKind::H3Cell,
        token: "h3",
        item_name: "cell",
        label: "H3 cell id",
        class_name: "H3Cell",
        format: ParseFormat::H3,
    },
    GridMeta {
        kind: GridKind::S2Cell,
        token: "s2",
        item_name: "cell",
        label: "S2 cell id",
        class_name: "S2Cell",
        format: ParseFormat::S2,
    },
    GridMeta {
        kind: GridKind::Tile,
        token: "tile",
        item_name: "tile",
        label: "tile id",
        class_name: "Tile",
        format: ParseFormat::Tile,
    },
    GridMeta {
        kind: GridKind::GeohashCell,
        token: "geohash",
        item_name: "cell",
        label: "geohash cell token",
        class_name: "GeohashCell",
        format: ParseFormat::Geohash,
    },
];

pub(crate) fn invalid_cell_id(kind: GridKind, id: impl std::fmt::Display) -> PyErr {
    let meta = kind.meta();
    parse_error(format!("invalid {} {id}", meta.label), meta.format)
}

pub(crate) fn uncompact_floor_error(
    kind: GridKind,
    depth_name: &str,
    token: impl std::fmt::Display,
) -> PyErr {
    let item = kind.meta().item_name;
    GeometryError::new_err(format!(
        "uncompact {depth_name} must be >= every {item}'s {depth_name}; {item} {token} is finer"
    ))
}

impl GridKind {
    const fn idx(self) -> usize {
        self as usize
    }

    const fn meta(self) -> &'static GridMeta {
        &GRID_META[self.idx()]
    }

    pub(super) const fn token(self) -> &'static str {
        self.meta().token
    }

    const fn parse_format(self) -> ParseFormat {
        self.meta().format
    }

    fn invalid_id(self, id: impl std::fmt::Display) -> PyErr {
        invalid_cell_id(self, id)
    }

    pub(super) fn parse(token: &str) -> PyResult<Self> {
        GRID_META
            .iter()
            .find(|meta| meta.token == token)
            .map(|meta| meta.kind)
            .ok_or_else(|| GeometryError::new_err(format!("unknown cell array grid {token:?}")))
    }

    pub(super) const fn class_name(self) -> &'static str {
        self.meta().class_name
    }

    pub(crate) fn validate_id(self, id: u64) -> PyResult<()> {
        match self {
            Self::H3Cell => {
                super::h3::validate_h3_index_id::<CellIndex>(id)?;
            },
            Self::S2Cell => {
                CellId::from_raw(id).ok_or_else(|| self.invalid_id(id))?;
            },
            Self::Tile => {
                Tile::from_id(id).ok_or_else(|| self.invalid_id(id))?;
            },
            Self::GeohashCell => {
                geohash_from_identity_key(id).map_err(|()| self.invalid_id(id))?;
            },
        }
        Ok(())
    }

    pub(crate) fn cell_from_id(self, py: Python<'_>, id: u64) -> PyResult<Bound<'_, PyAny>> {
        self.validate_id(id)?;
        match self {
            Self::H3Cell => {
                let cell = PyH3Cell {
                    cell: CellIndex::try_from(id).map_err(|_| self.invalid_id(id))?,
                };
                Ok(Bound::new(py, cell)?.into_any())
            },
            Self::S2Cell => {
                let cell = PyS2Cell {
                    cell: CellId::from_raw(id).ok_or_else(|| self.invalid_id(id))?,
                };
                Ok(Bound::new(py, cell)?.into_any())
            },
            Self::Tile => {
                let cell = PyTile {
                    cell: Tile::from_id(id).ok_or_else(|| self.invalid_id(id))?,
                };
                Ok(Bound::new(py, cell)?.into_any())
            },
            Self::GeohashCell => {
                let cell = PyGeohashCell {
                    cell: geohash_from_identity_key(id).map_err(|()| self.invalid_id(id))?,
                };
                Ok(Bound::new(py, cell)?.into_any())
            },
        }
    }

    pub(crate) fn id_from_value(self, value: &Bound<'_, PyAny>) -> PyResult<u64> {
        if self != Self::GeohashCell
            && value.cast_exact::<PyBool>().is_err()
            && value.cast::<PyInt>().is_ok()
        {
            let id = value.extract::<u64>().map_err(|_| {
                parse_error(
                    format!(
                        "{} id must be a non-negative 64-bit integer",
                        self.class_name()
                    ),
                    self.parse_format(),
                )
            })?;
            self.validate_id(id)?;
            return Ok(id);
        }
        match self {
            Self::H3Cell => h3_id_from_object(value),
            Self::S2Cell => s2_id_from_object(value),
            Self::Tile => tile_id_from_value(value),
            Self::GeohashCell => geohash_id_from_object(value),
        }
    }
}

macro_rules! id_from_object {
    (
        $function:ident {
            string_attr: $string_attr:literal,
            validate: |$id:ident| $validate:expr,
            parse_text: |$text:ident| $parse_text:expr $(,)?
        }
    ) => {
        fn $function(value: &Bound<'_, PyAny>) -> PyResult<u64> {
            let id_attr = value.getattr("id").ok();
            if let Some(id_attr) = id_attr.as_ref()
                && let Ok($id) = id_attr.extract::<u64>()
            {
                $validate?;
                return Ok($id);
            }
            let $text: String = if $string_attr == "id" {
                id_attr.as_ref().map_or_else(
                    || value.extract(),
                    |attr| attr.extract().or_else(|_| value.extract()),
                )?
            } else {
                value.getattr($string_attr).map_or_else(
                    |_| value.extract(),
                    |attr| attr.extract().or_else(|_| value.extract()),
                )?
            };
            $parse_text
        }
    };
}

id_from_object! {
    h3_id_from_object {
        string_attr: "id",
        validate: |id| super::h3::validate_h3_index_id::<CellIndex>(id).map(|_| ()),
        parse_text: |text| <CellIndex as super::h3::H3Index>::from_token(&text)
            .map(u64::from)
            .ok_or_else(|| invalid_cell_id(GridKind::H3Cell, format_args!("{text:?}"))),
    }
}

id_from_object! {
    s2_id_from_object {
        string_attr: "token",
        validate: |id| CellId::from_raw(id)
            .map(|_| ())
            .ok_or_else(|| invalid_cell_id(GridKind::S2Cell, id)),
        parse_text: |text| CellId::from_token(&text)
            .map(CellId::raw)
            .ok_or_else(|| invalid_cell_id(GridKind::S2Cell, format_args!("{text:?}"))),
    }
}

fn geohash_id_from_object(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    if let Ok(cell) = value.extract::<PyRef<'_, PyGeohashCell>>() {
        return Ok(cell.cell.identity_key());
    }
    if value.cast_exact::<PyInt>().is_ok() {
        return Err(PyTypeError::new_err(
            "geohash cells are constructed from tokens, not integers",
        ));
    }
    let text: String = value
        .extract()
        .map_err(|_| PyTypeError::new_err("geohash cells are constructed from string tokens"))?;
    let cell = Geohash::parse(&text).map_err(|message| {
        crate::py::errors::tag_parse_format(
            crate::py::errors::ParseError::new_err(message),
            ParseFormat::Geohash,
        )
    })?;
    Ok(cell.identity_key())
}

fn tile_id_from_value(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    if let Ok(id_attr) = value.getattr("id")
        && id_attr.is_instance_of::<PyInt>()
    {
        let id = id_attr.extract::<u64>()?;
        return Tile::from_id(id)
            .map(Tile::id)
            .ok_or_else(|| invalid_cell_id(GridKind::Tile, id));
    }
    if value.cast_exact::<PyBool>().is_err() && value.cast::<PyInt>().is_ok() {
        let id = value.extract::<u64>().map_err(|_| {
            parse_error(
                "tile id must be a non-negative 64-bit integer",
                ParseFormat::Tile,
            )
        })?;
        return Tile::from_id(id)
            .map(Tile::id)
            .ok_or_else(|| invalid_cell_id(GridKind::Tile, id));
    }
    let text: String = value
        .extract()
        .map_err(|_| PyTypeError::new_err("tile must be a Tile, integer id, or string quadkey"))?;
    Ok(Tile::from_quadkey(&text)
        .map_err(|message| {
            crate::py::errors::tag_parse_format(
                crate::py::errors::ParseError::new_err(message),
                ParseFormat::Quadkey,
            )
        })?
        .id())
}

pub(super) fn geohash_from_identity_key(id: u64) -> Result<Geohash, ()> {
    let precision = (id & 0xF) as u8;
    if !(1..=GEOHASH_MAX_PRECISION).contains(&precision) {
        return Err(());
    }
    let bits = id & !0xF;
    let cell = Geohash { bits, precision };
    if cell.identity_key() != id {
        return Err(());
    }
    // Canonical geohash: the 5*precision data bits are left-aligned in the top
    // of `bits`; the region below them (down to the precision nibble, already
    // cleared in `bits`) must be zero. `mask` is the low (64 - 5*precision)
    // bits, so any bit set there marks a non-canonical id.
    let used = u32::from(precision) * 5;
    if used < 64 {
        let mask = (1_u64 << (64 - used)) - 1;
        if bits & mask != 0 {
            return Err(());
        }
    }
    Ok(cell)
}

pub(super) fn grid_kind_from_type(cell_type: &Bound<'_, PyType>) -> PyResult<GridKind> {
    let py = cell_type.py();
    if cell_type.is(py.get_type::<PyH3Cell>()) {
        return Ok(GridKind::H3Cell);
    }
    if cell_type.is(py.get_type::<PyS2Cell>()) {
        return Ok(GridKind::S2Cell);
    }
    if cell_type.is(py.get_type::<PyTile>()) {
        return Ok(GridKind::Tile);
    }
    if cell_type.is(py.get_type::<PyGeohashCell>()) {
        return Ok(GridKind::GeohashCell);
    }
    Err(PyTypeError::new_err(
        "type must be the native H3Cell, S2Cell, Tile, or GeohashCell class",
    ))
}

fn grid_kind_from_cell(value: &Bound<'_, PyAny>) -> Option<GridKind> {
    if value.cast_exact::<PyH3Cell>().is_ok() {
        return Some(GridKind::H3Cell);
    }
    if value.cast_exact::<PyS2Cell>().is_ok() {
        return Some(GridKind::S2Cell);
    }
    if value.cast_exact::<PyTile>().is_ok() {
        return Some(GridKind::Tile);
    }
    value
        .cast_exact::<PyGeohashCell>()
        .is_ok()
        .then_some(GridKind::GeohashCell)
}

fn parse_uint64_ids(ids: &Bound<'_, PyAny>) -> PyResult<Option<Vec<u64>>> {
    let Ok(array) = ids.cast::<PyUntypedArray>() else {
        return Ok(None);
    };
    if array.ndim() > 1 {
        return Err(PyTypeError::new_err(
            "cell array ids must be zero- or one-dimensional",
        ));
    }
    macro_rules! try_array {
        ($($ty:ty),* $(,)?) => {
            $(
                if let Ok(values) = ids.extract::<PyReadonlyArrayDyn<'_, $ty>>() {
                    let flat = values.as_array();
                    // Fallible reserve: a small-dtype array must not amplify
                    // into an infallible `Vec<u64>` of the same length.
                    let mut out = crate::try_vec_with_capacity(flat.len())?;
                    for &value in flat.iter() {
                        let id = u64::try_from(value).map_err(|_| {
                            PyTypeError::new_err(
                                "cell array ids must be integers representable as uint64",
                            )
                        })?;
                        out.push(id);
                    }
                    return Ok(Some(out));
                }
            )*
        };
    }
    try_array!(u8, u16, u32, u64, usize);
    try_array!(i8, i16, i32, i64, isize);
    if array
        .getattr("dtype")?
        .getattr("kind")?
        .extract::<String>()?
        == "f"
    {
        return Err(PyTypeError::new_err(
            "cell array ids must be integers representable as uint64, not floats",
        ));
    }
    // Object / string / other dtypes are not packed uint64 columns — fall
    // through so `collect_ids` walks them as an ordinary iterable of cells,
    // tokens, or ids (e.g. `np.array([cell], dtype=object)`).
    Ok(None)
}

pub(super) fn collect_ids(ids: &Bound<'_, PyAny>, kind: GridKind) -> PyResult<Vec<u64>> {
    if ids.is_instance_of::<pyo3::types::PyBytes>()
        || ids.is_instance_of::<pyo3::types::PyByteArray>()
        || (ids.is_instance_of::<pyo3::types::PyMemoryView>()
            && matches!(
                ids.getattr("format")?.extract::<String>()?.as_str(),
                "B" | "b" | "c"
            ))
    {
        return Err(PyTypeError::new_err(
            "byte payloads are not cell collections; use payload.decode() for one textual token or a list/uint64 array for numeric ids",
        ));
    }
    // A token is an atomic cell identity for every grid. Treating bare text
    // as an iterable silently changes e.g. "u3" into two precision-one
    // geohashes (and loses Tile(""), the valid root). There is no principled
    // way to choose one token versus a collection of one-character tokens.
    if ids.is_instance_of::<PyString>() {
        let token: String = ids.extract()?;
        return Err(PyTypeError::new_err(format!(
            "bare token text is ambiguous; use [{token:?}] for one cell or an explicit iterable for many"
        )));
    }
    if let Some(values) = parse_uint64_ids(ids)? {
        if kind == GridKind::GeohashCell {
            return Err(PyTypeError::new_err(
                "geohash cells are constructed from tokens, not integers",
            ));
        }
        for &id in &values {
            kind.validate_id(id)?;
        }
        return Ok(values);
    }
    if let Ok(buffer) = PyBuffer::<u64>::get(ids) {
        if kind == GridKind::GeohashCell {
            return Err(PyTypeError::new_err(
                "geohash cells are constructed from tokens, not integers",
            ));
        }
        // m07: same rank rule as ndarray — CellArray is a 0/1-D id column.
        // A multi-dimensional memoryview must not flatten while ndarray rejects.
        if buffer.dimensions() > 1 {
            return Err(PyTypeError::new_err(
                "cell array ids must be zero- or one-dimensional",
            ));
        }
        let count = buffer.item_count();
        let mut values = crate::try_vec_with_capacity(count)?;
        // Copy via the buffer API into a fallibly reserved Vec; normalize
        // non-native endian (e.g. NumPy `>u8`) so ids are not silently corrupted.
        values.resize(count, 0);
        crate::buffer_copy_to_slice_u64(ids.py(), &buffer, &mut values)?;
        for &id in &values {
            kind.validate_id(id)?;
        }
        return Ok(values);
    }
    let iter = ids
        .try_iter()
        .map_err(|_| PyTypeError::new_err("cell array ids must be an iterable"))?;
    let mut values = Vec::new();
    for item in iter {
        let item = item?;
        if let Some(item_kind) = grid_kind_from_cell(&item)
            && item_kind != kind
        {
            return Err(PyTypeError::new_err(format!(
                "cell array type={} does not match {} value",
                kind.class_name(),
                item_kind.class_name(),
            )));
        }
        crate::try_push(&mut values, kind.id_from_value(&item)?)?;
    }
    Ok(values)
}

pub(super) fn collect_inferred_ids(values: &Bound<'_, PyAny>) -> PyResult<(GridKind, Vec<u64>)> {
    if values.is_instance_of::<pyo3::types::PyBytes>()
        || values.is_instance_of::<pyo3::types::PyByteArray>()
        || (values.is_instance_of::<pyo3::types::PyMemoryView>()
            && matches!(
                values.getattr("format")?.extract::<String>()?.as_str(),
                "B" | "b" | "c"
            ))
    {
        return Err(PyTypeError::new_err(
            "byte payloads are not cell collections; use payload.decode() for one textual token or a list/uint64 array for numeric ids",
        ));
    }
    if values.is_instance_of::<PyString>() {
        let token: String = values.extract()?;
        return Err(PyTypeError::new_err(format!(
            "bare token text is ambiguous; use [{token:?}] for one cell or an explicit iterable for many"
        )));
    }
    if PyBuffer::<u64>::get(values).is_ok() {
        return Err(PyTypeError::new_err(
            "cell array type is required for raw id arrays and buffers",
        ));
    }
    let iter = values.try_iter().map_err(|_| {
        PyTypeError::new_err("cell array values must be an iterable of typed cell objects")
    })?;
    let mut kind = None;
    let mut ids = Vec::new();
    for item in iter {
        let item = item?;
        let item_kind = grid_kind_from_cell(&item).ok_or_else(|| {
            PyTypeError::new_err(
                "cell array type is required for raw ids, tokens, and mixed or ambiguous values",
            )
        })?;
        if let Some(kind) = kind {
            if item_kind != kind {
                return Err(PyTypeError::new_err(format!(
                    "cell array values must contain one cell type; found {} and {}",
                    kind.class_name(),
                    item_kind.class_name(),
                )));
            }
        } else {
            kind = Some(item_kind);
        }
        crate::try_push(&mut ids, item_kind.id_from_value(&item)?)?;
    }
    let kind =
        kind.ok_or_else(|| PyTypeError::new_err("cell array type is required for an empty input"))?;
    Ok((kind, ids))
}
