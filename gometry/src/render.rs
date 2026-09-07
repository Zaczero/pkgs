#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! SVG rendering of geometries for Jupyter/`_repr_svg_`. Pure geometry →
//! string; no `PyO3`. The Y axis is flipped so geographic north points up.

use crate::error::{ResourceError, Result};
use crate::geometry::{Bounds, CoordSeq, Coordinates, Point, Polygon, Shape};

struct TryString(String);

impl TryString {
    const fn new() -> Self {
        Self(String::new())
    }

    #[expect(
        clippy::same_name_method,
        reason = "shadows `fmt::Write::write_fmt` so `write!` on this buffer yields the crate's fallible result"
    )]
    fn write_fmt(&mut self, args: std::fmt::Arguments<'_>) -> Result<()> {
        std::fmt::write(self, args).map_err(|_| ResourceError::Allocation.into())
    }

    fn finish(self) -> String {
        self.0
    }
}

impl std::fmt::Write for TryString {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0
            .try_reserve(value.len())
            .map_err(|_| std::fmt::Error)?;
        self.0.push_str(value);
        Ok(())
    }
}

pub(crate) fn html_text_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

/// Pixel budget for the longest side of a rendered SVG.
const SVG_MAX_PX: f64 = 300.0;

/// Margin around the drawing, in the pixel frame. See `render_shape_svg` for
/// why it cannot be a fraction of the world extent.
const SVG_PAD_PX: f64 = SVG_MAX_PX * 0.05;

/// Bounds `(minx, miny, maxx, maxy)` for the drawing. Degenerate (point /
/// zero-extent) inputs are widened so the `viewBox` is never empty. Extreme-
/// but-finite coordinates are admitted only when the width and height stay
/// finite (otherwise the SVG path uses a local unit frame).
fn svg_bounds(shape: &Shape) -> Option<Bounds> {
    let bounds = shape.bounds()?;
    let (mut minx, mut miny, mut maxx, mut maxy) =
        (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy());
    if ![minx, miny, maxx, maxy].iter().all(|v| v.is_finite()) {
        return None;
    }
    let width = maxx - minx;
    let height = maxy - miny;
    if !width.is_finite() || !height.is_finite() {
        return None;
    }
    if width <= 0.0 {
        let pad = if height > 0.0 { height * 0.5 } else { 1.0 };
        minx -= pad;
        maxx += pad;
    }
    if height <= 0.0 {
        let widened = maxx - minx;
        let pad = if widened > 0.0 { widened * 0.5 } else { 1.0 };
        miny -= pad;
        maxy += pad;
    }
    let out = Bounds::new_raw(minx, miny, maxx, maxy);
    let (min_x, min_y, max_x, max_y) = out.into_tuple();
    ([min_x, min_y, max_x, max_y].iter().all(|v| v.is_finite())
        && (max_x - min_x).is_finite()
        && (max_y - min_y).is_finite())
    .then_some(out)
}

/// Render a shape to a standalone SVG document. The Y axis is flipped (so geo
/// north points up) via a `matrix(1 0 0 -1 0 dy)` transform on a wrapping `g`.
/// HTML grid of inline SVG previews for a geometry array.
pub(crate) fn geometry_array_svg_grid_html_masked<'a>(
    rows: impl Iterator<Item = crate::array::ArrayRow<'a, ()>>,
    n: usize,
    preview: usize,
) -> Result<String> {
    let mut cells = TryString::new();
    for row in rows.take(preview) {
        let svg = match row {
            crate::array::ArrayRow::Missing { .. } => missing_svg()?,
            crate::array::ArrayRow::Present(row) => row.row().with_shape(render_shape_svg)?,
        };
        cells.write_fmt(format_args!(
            "<div style=\"display:inline-block;margin:2px\">{svg}</div>"
        ))?;
    }
    let mut output = TryString::new();
    output.write_fmt(format_args!(
        "<div class=\"gometry-geom-array\"><div>{n} geometries"
    ))?;
    if n > preview {
        output.write_fmt(format_args!(" (showing first {preview})"))?;
    }
    output.write_fmt(format_args!(
        "</div><div style=\"display:flex;flex-wrap:wrap\">{}</div></div>",
        cells.finish()
    ))?;
    Ok(output.finish())
}

fn missing_svg() -> Result<String> {
    crate::boundary::try_string_from_str(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" class=\"gometry-geom\" viewBox=\"0 0 120 80\">\
         <rect x=\"1\" y=\"1\" width=\"118\" height=\"78\" fill=\"#f7f7f7\" stroke=\"#d0d0d0\"/>\
         <text x=\"60\" y=\"44\" text-anchor=\"middle\" font-size=\"14\" fill=\"#777\">missing</text>\
         </svg>",
    )
    .map_err(|()| ResourceError::Allocation.into())
}

pub(crate) fn render_shape_svg(shape: &Shape) -> Result<String> {
    const EMPTY: &str = "<svg xmlns=\"http://www.w3.org/2000/svg\" class=\"gometry-geom\"/>";
    if shape.is_empty() {
        return crate::boundary::try_string_from_str(EMPTY)
            .map_err(|()| ResourceError::Allocation.into());
    }
    let Some(bounds) = svg_bounds(shape) else {
        return crate::boundary::try_string_from_str(EMPTY)
            .map_err(|()| ResourceError::Allocation.into());
    };
    let (minx, miny, maxx, maxy) = bounds.into_tuple();
    let w = maxx - minx;
    let h = maxy - miny;
    if !w.is_finite() || !h.is_finite() || w <= 0.0 || h <= 0.0 {
        return crate::boundary::try_string_from_str(EMPTY)
            .map_err(|()| ResourceError::Allocation.into());
    }
    // Map world → local pixel frame so extreme-but-finite coordinates never
    // produce infinite viewBox/stroke or NaN transforms.
    let scale = SVG_MAX_PX / w.max(h);
    if !scale.is_finite() || scale <= 0.0 {
        return crate::boundary::try_string_from_str(EMPTY)
            .map_err(|()| ResourceError::Allocation.into());
    }
    let px_w = (w * scale).max(1.0);
    let px_h = (h * scale).max(1.0);
    if !px_w.is_finite() || !px_h.is_finite() {
        return crate::boundary::try_string_from_str(EMPTY)
            .map_err(|()| ResourceError::Allocation.into());
    }
    let sw = 1.5_f64;
    let r = 3.0_f64;
    // The margin lives in the pixel frame because the things it has to contain
    // do: strokes and vertex markers are sized in pixels, while a margin taken
    // as a fraction of each world axis is scaled by that axis. On a shape much
    // wider than it is tall the vertical share shrinks below the marker radius
    // and the browser clips half a marker off the edge.
    let pad = SVG_PAD_PX.max(r + sw);
    let box_w = px_w + 2.0 * pad;
    let box_h = px_h + 2.0 * pad;
    let flip_y = px_h + pad;
    let valid = shape.validate().is_none();
    let stroke = if valid { "#22c55e" } else { "#ef4444" };
    let fill = if valid {
        "rgba(34,197,94,0.25)"
    } else {
        "rgba(239,68,68,0.25)"
    };
    let body = svg_elements_local(shape, stroke, fill, sw, r, minx, miny, scale)?;
    // The flip about `flip_y` puts y increasing up and insets the drawing by
    // `pad` on every side of the viewBox.
    let mut output = TryString::new();
    output.write_fmt(format_args!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" class=\"gometry-geom\" \
         width=\"{box_w:.0}\" height=\"{box_h:.0}\" viewBox=\"0 0 {box_w:.6} {box_h:.6}\" \
         preserveAspectRatio=\"xMidYMid meet\">\
         <g transform=\"matrix(1 0 0 -1 {pad:.6} {flip_y:.6})\">{body}</g></svg>"
    ))?;
    Ok(output.finish())
}

pub(crate) fn render_shape_html(shape: &Shape, repr: &str) -> Result<String> {
    let svg = render_shape_svg(shape)?;
    let repr = html_text_escape(repr);
    let mut output = TryString::new();
    output.write_fmt(format_args!(
        "<div class=\"gometry-geom-html\"><div>{repr}</div>{svg}</div>"
    ))?;
    Ok(output.finish())
}

fn map_xy(x: f64, y: f64, minx: f64, miny: f64, scale: f64) -> (f64, f64) {
    ((x - minx) * scale, (y - miny) * scale)
}

fn svg_path_points_local<C: Coordinates + ?Sized>(
    points: &C,
    close: bool,
    minx: f64,
    miny: f64,
    scale: f64,
) -> Result<String> {
    let mut s = TryString::new();
    for (i, p) in points.iter_coords().enumerate() {
        let (x, y) = map_xy(p.x, p.y, minx, miny, scale);
        if !x.is_finite() || !y.is_finite() {
            continue;
        }
        let cmd = if i == 0 { 'M' } else { 'L' };
        s.write_fmt(format_args!("{cmd}{x:.6} {y:.6} "))?;
    }
    if close {
        s.write_fmt(format_args!("Z"))?;
    }
    let mut output = s.finish();
    output.truncate(output.trim_end().len());
    Ok(output)
}

fn svg_elements_local(
    shape: &Shape,
    stroke: &str,
    fill: &str,
    sw: f64,
    r: f64,
    minx: f64,
    miny: f64,
    scale: f64,
) -> Result<String> {
    let mut out = TryString::new();
    let circle = |out: &mut TryString, p: &Point| -> Result<()> {
        let (x, y) = map_xy(p.x, p.y, minx, miny, scale);
        out.write_fmt(format_args!(
            "<circle cx=\"{x:.6}\" cy=\"{y:.6}\" r=\"{r:.6}\" fill=\"{stroke}\"/>"
        ))
    };
    let polyline = |out: &mut TryString, pts: &CoordSeq| -> Result<()> {
        let path = svg_path_points_local(pts, false, minx, miny, scale)?;
        out.write_fmt(format_args!(
            "<path d=\"{path}\" fill=\"none\" stroke=\"{stroke}\" stroke-width=\"{sw:.6}\"/>"
        ))
    };
    let polygon_path = |out: &mut TryString, polygon: &Polygon| -> Result<()> {
        let mut d = TryString::new();
        d.write_fmt(format_args!(
            "{}",
            svg_path_points_local(&polygon.shell, true, minx, miny, scale)?
        ))?;
        for hole in polygon.holes.iter() {
            d.write_fmt(format_args!(
                " {}",
                svg_path_points_local(hole, true, minx, miny, scale)?
            ))?;
        }
        let d = d.finish();
        out.write_fmt(format_args!(
            "<path fill-rule=\"evenodd\" d=\"{d}\" fill=\"{fill}\" stroke=\"{stroke}\" stroke-width=\"{sw:.6}\"/>"
        ))
    };
    match shape {
        Shape::Point(p) => circle(&mut out, p)?,
        Shape::MultiPoint(points) => {
            for p in points {
                circle(&mut out, &p)?;
            }
        },
        Shape::LineString(points) => polyline(&mut out, points)?,
        Shape::MultiLineString(lines) => {
            for line in lines {
                polyline(&mut out, line)?;
            }
        },
        Shape::Polygon(polygon) => polygon_path(&mut out, polygon)?,
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                polygon_path(&mut out, polygon)?;
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                let elements =
                    svg_elements_local(geometry, stroke, fill, sw, r, minx, miny, scale)?;
                out.write_fmt(format_args!("{elements}"))?;
            }
        },
        Shape::Empty(..) => {},
    }
    Ok(out.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordinateAxes, MultiMember, MultiPoint};

    #[test]
    fn multipoint_rendering_skips_embedded_empty_members() {
        let shape = Shape::MultiPoint(
            MultiPoint::try_new(CoordinateAxes::XY, vec![
                MultiMember::Empty,
                MultiMember::Value(Point::new_raw_xy(1.0, 2.0)),
                MultiMember::Empty,
                MultiMember::Value(Point::new_raw_xy(3.0, 4.0)),
            ])
            .expect("homogeneous multipoint members"),
        );

        let svg = render_shape_svg(&shape).expect("render valid multipoint");
        assert_eq!(svg.matches("<circle ").count(), 2);
    }
}
