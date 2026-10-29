//! Protobuf field framing and MVT command streams.

use std::collections::HashSet;

use prost::Message;
use prost::bytes::Buf as _;
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Value {
    #[prost(string, optional, tag = "1")]
    pub string: Option<String>,
    #[prost(float, optional, tag = "2")]
    pub float: Option<f32>,
    #[prost(double, optional, tag = "3")]
    pub double: Option<f64>,
    #[prost(int64, optional, tag = "4")]
    pub int: Option<i64>,
    #[prost(uint64, optional, tag = "5")]
    pub uint: Option<u64>,
    #[prost(sint64, optional, tag = "6")]
    pub sint: Option<i64>,
    #[prost(bool, optional, tag = "7")]
    pub boolean: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Feature {
    #[prost(uint64, optional, tag = "1")]
    pub id: Option<u64>,
    #[prost(uint32, repeated, packed = "true", tag = "2")]
    pub tags: Vec<u32>,
    #[prost(uint32, optional, tag = "3")]
    pub kind: Option<u32>,
    #[prost(uint32, repeated, packed = "true", tag = "4")]
    pub geometry: Vec<u32>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Layer {
    #[prost(string, optional, tag = "1")]
    pub name: Option<String>,
    #[prost(message, repeated, tag = "2")]
    pub features: Vec<Feature>,
    #[prost(string, repeated, tag = "3")]
    pub keys: Vec<String>,
    #[prost(message, repeated, tag = "4")]
    pub values: Vec<Value>,
    #[prost(uint32, optional, tag = "5")]
    pub extent: Option<u32>,
    #[prost(uint32, optional, tag = "15")]
    pub version: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Tile {
    #[prost(message, repeated, tag = "3")]
    pub layers: Vec<Layer>,
}

#[derive(Clone, PartialEq, Message)]
struct Header {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(uint32, optional, tag = "15")]
    version: Option<u32>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Geometry {
    Points(Vec<[i64; 2]>),
    Lines(Vec<Vec<[i64; 2]>>),
    Rings(Vec<Vec<[i64; 2]>>),
}

fn command(words: &[u32], at: &mut usize) -> Result<(u32, usize), &'static str> {
    let value = *words.get(*at).ok_or("missing geometry command")?;
    *at = at.wrapping_add(1);
    let count = (value >> 3) as usize;
    if count == 0 {
        return Err("zero geometry command count");
    }
    Ok((value & 7, count))
}

fn positions(
    words: &[u32],
    at: &mut usize,
    count: usize,
    cursor: &mut [i64; 2],
    nonzero: bool,
    output: &mut Vec<[i64; 2]>,
) -> Result<(), &'static str> {
    let end = at
        .checked_add(count.checked_mul(2).ok_or("command count overflow")?)
        .ok_or("command count overflow")?;
    let chunk = words.get(*at..end).ok_or("missing geometry coordinates")?;
    for pair in chunk.as_chunks::<2>().0 {
        if nonzero && *pair == [0, 0] {
            return Err("zero-length line step");
        }
        for (coordinate, &encoded) in cursor.iter_mut().zip(pair) {
            let delta = i64::from(encoded >> 1) ^ i64::from(encoded & 1).wrapping_neg();
            if delta == i64::from(i32::MIN) {
                return Err("geometry delta out of range");
            }
            *coordinate = coordinate.checked_add(delta).ok_or("coordinate overflow")?;
            let magnitude = coordinate.unsigned_abs();
            let bits = magnitude.bit_width();
            if bits > 53 && magnitude.trailing_zeros() < bits.wrapping_sub(53) {
                return Err("coordinate is not exactly representable");
            }
        }
        output.push(*cursor);
    }
    *at = end;
    Ok(())
}

pub(crate) fn decode_geometry(kind: u32, words: &[u32]) -> Result<Geometry, &'static str> {
    let mut at = 0;
    let mut cursor = [0, 0];
    match kind {
        1 => {
            let (id, count) = command(words, &mut at)?;
            if id != 1 {
                return Err("point must start with MoveTo");
            }
            let mut points = Vec::new();
            positions(words, &mut at, count, &mut cursor, false, &mut points)?;
            if at != words.len() {
                return Err("extra point commands");
            }
            Ok(Geometry::Points(points))
        },
        2 | 3 => {
            let mut parts = Vec::new();
            while at < words.len() {
                if command(words, &mut at)? != (1, 1) {
                    return Err("part must start with one MoveTo");
                }
                let mut part = Vec::new();
                positions(words, &mut at, 1, &mut cursor, false, &mut part)?;
                let (id, count) = command(words, &mut at)?;
                if id != 2 || (kind == 3 && count < 2) {
                    return Err("part requires LineTo");
                }
                positions(words, &mut at, count, &mut cursor, true, &mut part)?;
                if kind == 3 {
                    if command(words, &mut at)? != (7, 1) {
                        return Err("ring requires ClosePath");
                    }
                    if part.first() == part.last() {
                        return Err("ring repeats its first vertex");
                    }
                }
                parts.push(part);
            }
            if parts.is_empty() {
                return Err("geometry has no parts");
            }
            Ok(if kind == 2 {
                Geometry::Lines(parts)
            } else {
                Geometry::Rings(parts)
            })
        },
        _ => Err("unsupported geometry type"),
    }
}

fn write_command(emit: &mut impl FnMut(u32), id: u32, count: usize) -> Result<(), &'static str> {
    let count = u32::try_from(count).map_err(|_| "command count overflow")?;
    if count == 0 || count >= (1 << 29) {
        return Err("command count out of range");
    }
    emit((count << 3) | id);
    Ok(())
}

fn write_position(
    emit: &mut impl FnMut(u32),
    cursor: &mut [i64; 2],
    position: [i64; 2],
    nonzero: bool,
) -> Result<(), &'static str> {
    if nonzero && *cursor == position {
        return Err("zero-length line step");
    }
    for (&old, &new) in cursor.iter().zip(&position) {
        let delta = i128::from(new).wrapping_sub(i128::from(old));
        if delta < i128::from(i32::MAX).wrapping_neg() || delta > i128::from(i32::MAX) {
            return Err("geometry delta out of range");
        }
        let delta = delta as i32;
        emit(((delta << 1) ^ (delta >> 31)) as u32);
    }
    *cursor = position;
    Ok(())
}

pub(crate) fn encode_geometry(geometry: &Geometry) -> Result<(u32, Vec<u32>), &'static str> {
    let capacity = match geometry {
        Geometry::Points(points) => points.len().saturating_mul(2).saturating_add(1),
        Geometry::Lines(parts) | Geometry::Rings(parts) => {
            let commands = if matches!(geometry, Geometry::Rings(_)) {
                3
            } else {
                2
            };
            parts.iter().fold(0_usize, |count, part| {
                count.saturating_add(part.len().saturating_mul(2).saturating_add(commands))
            })
        },
    };
    let mut words = Vec::with_capacity(capacity);
    let kind = write_geometry(geometry, &mut |word| words.push(word))?;
    Ok((kind, words))
}

pub(crate) fn validate_geometry(geometry: &Geometry) -> Result<(), &'static str> {
    write_geometry(geometry, &mut |_| {}).map(|_| ())
}

fn write_geometry(geometry: &Geometry, emit: &mut impl FnMut(u32)) -> Result<u32, &'static str> {
    let mut cursor = [0, 0];
    match geometry {
        Geometry::Points(points) => {
            write_command(emit, 1, points.len())?;
            for &point in points {
                write_position(emit, &mut cursor, point, false)?;
            }
            Ok(1)
        },
        Geometry::Lines(parts) | Geometry::Rings(parts) => {
            if parts.is_empty() {
                return Err("geometry has no parts");
            }
            let polygon = matches!(geometry, Geometry::Rings(_));
            for part in parts {
                let Some((&start, rest)) = part.split_first() else {
                    return Err("empty geometry part");
                };
                if rest.is_empty() || (polygon && (rest.len() < 2 || part.first() == part.last())) {
                    return Err("invalid geometry part");
                }
                write_command(emit, 1, 1)?;
                write_position(emit, &mut cursor, start, false)?;
                write_command(emit, 2, rest.len())?;
                for &point in rest {
                    write_position(emit, &mut cursor, point, true)?;
                }
                if polygon {
                    write_command(emit, 7, 1)?;
                }
            }
            Ok(if polygon { 3 } else { 2 })
        },
    }
}

fn validate_value(value: &Value) -> Result<(), &'static str> {
    let count = [
        value.string.is_some(),
        value.float.is_some(),
        value.double.is_some(),
        value.int.is_some(),
        value.uint.is_some(),
        value.sint.is_some(),
        value.boolean.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if count != 1 {
        return Err("invalid value alternatives");
    }
    Ok(())
}

fn length_field<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let length = usize::try_from(decode_varint(input).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())?;
    if length > input.len() {
        return Err("truncated length-delimited field".into());
    }
    let (payload, rest) = input.split_at(length);
    *input = rest;
    Ok(payload)
}

fn require_wire(actual: WireType, expected: WireType) -> Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err("unexpected protobuf wire type".into())
    }
}

#[derive(Debug)]
pub(crate) struct DecodedFeature {
    pub id: Option<u64>,
    pub geometry: Geometry,
    pub tags: Vec<(usize, usize)>,
}

#[derive(Debug)]
pub(crate) struct DecodedLayer {
    pub name: String,
    pub extent: u32,
    pub keys: Vec<String>,
    pub values: Vec<Option<Value>>,
    pub features: Vec<DecodedFeature>,
}

pub(crate) fn decode_layer(
    body: &[u8],
    properties: Option<&[&str]>,
) -> Result<DecodedLayer, String> {
    let mut input = body;
    let mut name = None;
    let mut version = None;
    let mut extent = None;
    let mut keys = Vec::new();
    let mut value_messages = Vec::new();
    while input.has_remaining() {
        let (tag, wire) = decode_key(&mut input).map_err(|error| error.to_string())?;
        match tag {
            1 | 3 => {
                require_wire(wire, WireType::LengthDelimited)?;
                let text = std::str::from_utf8(length_field(&mut input)?)
                    .map_err(|error| error.to_string())?
                    .to_owned();
                if tag == 1 {
                    name = Some(text);
                } else {
                    keys.push(text);
                }
            },
            2 => {
                require_wire(wire, WireType::LengthDelimited)?;
                length_field(&mut input)?;
            },
            4 => {
                require_wire(wire, WireType::LengthDelimited)?;
                value_messages.push(length_field(&mut input)?);
            },
            5 | 15 => {
                require_wire(wire, WireType::Varint)?;
                let value =
                    u32::try_from(decode_varint(&mut input).map_err(|error| error.to_string())?)
                        .map_err(|error| error.to_string())?;
                if tag == 5 {
                    extent = Some(value);
                } else {
                    version = Some(value);
                }
            },
            _ => skip_field(wire, tag, &mut input, DecodeContext::default())
                .map_err(|error| error.to_string())?,
        }
    }
    let name = name.ok_or("missing layer name")?;
    let extent = extent
        .filter(|value| *value > 0)
        .ok_or("missing or zero extent")?;
    if version != Some(2) {
        return Err("unsupported or missing layer version".into());
    }
    let selected: Vec<bool> = keys
        .iter()
        .map(|key| properties.is_none_or(|names| names.contains(&key.as_str())))
        .collect();
    let mut values = vec![None; value_messages.len()];
    if properties.is_none() {
        for (slot, raw_value) in values.iter_mut().zip(&value_messages) {
            let value = Value::decode(*raw_value).map_err(|error| error.to_string())?;
            validate_value(&value).map_err(str::to_owned)?;
            *slot = Some(value);
        }
    }
    let mut features = Vec::new();
    let mut input = body;
    while input.has_remaining() {
        let (tag, wire) = decode_key(&mut input).map_err(|error| error.to_string())?;
        if tag != 2 {
            skip_field(wire, tag, &mut input, DecodeContext::default())
                .map_err(|error| error.to_string())?;
            continue;
        }
        require_wire(wire, WireType::LengthDelimited)?;
        let feature =
            Feature::decode(length_field(&mut input)?).map_err(|error| error.to_string())?;
        let mut seen = HashSet::new();
        let mut tags = Vec::new();
        let (pairs, remainder) = feature.tags.as_chunks::<2>();
        for &[key_index, value_index] in pairs {
            let key_index = key_index as usize;
            let value_index = value_index as usize;
            let key = keys.get(key_index).ok_or("key index out of range")?;
            if !seen.insert(key) {
                return Err("duplicate feature key".into());
            }
            let raw_value = value_messages
                .get(value_index)
                .ok_or("value index out of range")?;
            if *selected
                .get(key_index)
                .expect("tag key index was validated")
            {
                let value_slot = values
                    .get_mut(value_index)
                    .expect("tag value index was validated");
                if value_slot.is_none() {
                    let value = Value::decode(*raw_value).map_err(|error| error.to_string())?;
                    validate_value(&value).map_err(str::to_owned)?;
                    *value_slot = Some(value);
                }
                tags.push((key_index, value_index));
            }
        }
        if !remainder.is_empty() {
            return Err("odd tag count".into());
        }
        features.push(DecodedFeature {
            id: feature.id,
            geometry: decode_geometry(
                feature.kind.ok_or("missing geometry type")?,
                &feature.geometry,
            )
            .map_err(str::to_owned)?,
            tags,
        });
    }
    Ok(DecodedLayer {
        name,
        extent,
        keys,
        values,
        features,
    })
}

pub(crate) fn select_layers<'a>(
    mut input: &'a [u8],
    selected: Option<&HashSet<String>>,
) -> Result<Vec<(String, &'a [u8])>, String> {
    let mut output = Vec::new();
    let mut names = HashSet::new();
    while input.has_remaining() {
        let (tag, wire) = decode_key(&mut input).map_err(|error| error.to_string())?;
        if tag != 3 {
            skip_field(wire, tag, &mut input, DecodeContext::default())
                .map_err(|error| error.to_string())?;
            continue;
        }
        require_wire(wire, WireType::LengthDelimited)?;
        let body = length_field(&mut input)?;
        let header = Header::decode(body).map_err(|error| error.to_string())?;
        let name = header.name.ok_or("missing layer name")?;
        if !names.insert(name.clone()) {
            return Err("duplicate layer name".into());
        }
        if selected.is_none_or(|selected| selected.contains(&name)) {
            if header.version != Some(2) {
                return Err(format!("layer {name:?}: unsupported or missing version"));
            }
            output.push((name, body));
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_streams_match_independent_examples() {
        let point = Geometry::Points(vec![[25, 17]]);
        assert_eq!(decode_geometry(1, &[9, 50, 34]).unwrap(), point);
        assert_eq!(encode_geometry(&point).unwrap(), (1, vec![9, 50, 34]));
        let line = Geometry::Lines(vec![vec![[2, 2], [2, 10], [10, 10]]]);
        let words = vec![9, 4, 4, 18, 0, 16, 16, 0];
        assert_eq!(decode_geometry(2, &words).unwrap(), line);
        assert_eq!(encode_geometry(&line).unwrap(), (2, words));
    }

    #[test]
    fn close_path_does_not_move_cursor_between_rings() {
        let rings = Geometry::Rings(vec![
            vec![[0, 0], [10, 0], [10, 10], [0, 10]],
            vec![[11, 11], [20, 11], [20, 20], [11, 20]],
            vec![[13, 13], [13, 17], [17, 17], [17, 13]],
        ]);
        let words = vec![
            9, 0, 0, 26, 20, 0, 0, 20, 19, 0, 15, 9, 22, 2, 26, 18, 0, 0, 18, 17, 0, 15, 9, 4, 13,
            26, 0, 8, 8, 0, 0, 7, 15,
        ];
        assert_eq!(decode_geometry(3, &words).unwrap(), rings);
        assert_eq!(encode_geometry(&rings).unwrap(), (3, words));
    }

    #[test]
    fn malformed_and_out_of_range_commands_are_rejected() {
        for (kind, words) in [
            (1, vec![9, 4]),
            (1, vec![1]),
            (2, vec![9, 0, 0, 10, 0, 0]),
            (3, vec![9, 0, 0, 18, 2, 0, 0, 2]),
            (3, vec![9, 0, 0, 18, 2, 0, 1, 0, 15]),
            (1, vec![9, u32::MAX, 0]),
            (7, vec![9, 0, 0]),
        ] {
            assert!(decode_geometry(kind, &words).is_err(), "{kind}: {words:?}");
        }
        encode_geometry(&Geometry::Points(vec![[i64::from(i32::MIN), 0]]))
            .expect_err("out-of-range delta");
        encode_geometry(&Geometry::Lines(vec![vec![[2, 3], [2, 3]]]))
            .expect_err("zero-length step");
    }

    #[test]
    fn selected_properties_skip_unselected_value_contents() {
        let layer = Layer {
            name: Some("points".into()),
            features: vec![Feature {
                id: Some(0),
                tags: vec![0, 0, 1, 1],
                kind: Some(1),
                geometry: vec![9, 2, 4],
            }],
            keys: vec!["label".into(), "bad".into()],
            values: vec![
                Value {
                    string: Some("station".into()),
                    float: None,
                    double: None,
                    int: None,
                    uint: None,
                    sint: None,
                    boolean: None,
                },
                Value {
                    string: None,
                    float: None,
                    double: None,
                    int: None,
                    uint: None,
                    sint: None,
                    boolean: None,
                },
            ],
            extent: Some(4096),
            version: Some(2),
        };
        let body = layer.encode_to_vec();
        let selected = decode_layer(&body, Some(&["label"])).unwrap();
        assert_eq!(selected.features[0].id, Some(0));
        assert_eq!(selected.features[0].tags, vec![(0, 0)]);
        assert_eq!(
            selected.values[0].as_ref().unwrap().string.as_deref(),
            Some("station")
        );
        assert!(selected.values[1].is_none());
        assert!(
            decode_layer(&body, Some(&[]))
                .unwrap()
                .values
                .iter()
                .all(Option::is_none)
        );
        decode_layer(&body, None).expect_err("unselected malformed value is now required");
    }

    #[test]
    fn independent_tile_preserves_id_presence() {
        let hex = "1a570a0873746174696f6e73120d12040000010118012203091844121008001204000201031801220409709c011a046e616d651a046f70656e22090a0743656e7472616c2202380122060a0457657374220238002880207802";
        let bytes: Vec<u8> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let layers = select_layers(&bytes, Some(&HashSet::from(["stations".to_owned()]))).unwrap();
        let body = layers.first().unwrap().1;
        let layer = decode_layer(body, None).unwrap();
        assert_eq!(layer.name, "stations");
        assert_eq!(layer.extent, 4096);
        assert_eq!(layer.keys, ["name", "open"]);
        assert_eq!(layer.features[0].id, None);
        assert_eq!(layer.features[1].id, Some(0));
        assert_eq!(layer.features[0].geometry, Geometry::Points(vec![[12, 34]]));
        assert_eq!(layer.features[1].geometry, Geometry::Points(vec![[56, 78]]));
        select_layers(&bytes[..bytes.len() - 1], None).expect_err("truncated layer must fail");
    }
}
