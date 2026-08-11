#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! SVG rendering of geometries for Jupyter/`_repr_svg_`. Pure geometry →
//! string; no `PyO3`. The Y axis is flipped so geographic north points up.

use crate::geometry::{Bounds, CoordSeq, Coordinates, Point, Shape};

/// Pixel budget for the longest side of a rendered SVG.
const SVG_MAX_PX: f64 = 300.0;

/// Padded bounds `(minx, miny, maxx, maxy)` with ~5% margin. Degenerate
/// (point / zero-extent) inputs are widened so the `viewBox` is never empty.
/// Extreme-but-finite coordinates are admitted only when the padded width
/// and height stay finite (otherwise the SVG path uses a local unit frame).
fn svg_padded_bounds(shape: &Shape) -> Option<Bounds> {
    let bounds = shape.bounds()?;
    let (mut minx, mut miny, mut maxx, mut maxy) =
        (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy());
    if ![minx, miny, maxx, maxy].iter().all(|v| v.is_finite()) {
        return None;
    }
    let mut width = maxx - minx;
    let mut height = maxy - miny;
    if !width.is_finite() || !height.is_finite() {
        return None;
    }
    if width <= 0.0 {
        let pad = if height > 0.0 { height * 0.5 } else { 1.0 };
        minx -= pad;
        maxx += pad;
        width = maxx - minx;
    }
    if height <= 0.0 {
        let pad = if width > 0.0 { width * 0.5 } else { 1.0 };
        miny -= pad;
        maxy += pad;
        height = maxy - miny;
    }
    let pad_x = width * 0.05;
    let pad_y = height * 0.05;
    let out = Bounds::new_unchecked(minx - pad_x, miny - pad_y, maxx + pad_x, maxy + pad_y);
    let (min_x, min_y, max_x, max_y) = out.into_tuple();
    ([min_x, min_y, max_x, max_y].iter().all(|v| v.is_finite())
        && (max_x - min_x).is_finite()
        && (max_y - min_y).is_finite())
    .then_some(out)
}

/// Render a shape to a standalone SVG document. The Y axis is flipped (so geo
/// north points up) via a `matrix(1 0 0 -1 0 dy)` transform on a wrapping `g`.
/// HTML grid of inline SVG previews for a geometry array.
pub(crate) fn geometry_array_svg_grid_html_masked(
    rows: impl Iterator<Item = (bool, impl AsRef<Shape>)>,
    n: usize,
    preview: usize,
) -> String {
    use std::fmt::Write as _;
    let mut cells = String::new();
    for (missing, shape) in rows.take(preview) {
        let svg = if missing {
            missing_svg()
        } else {
            render_shape_svg(shape.as_ref())
        };
        let _ = write!(
            cells,
            "<div style=\"display:inline-block;margin:2px\">{svg}</div>"
        );
    }
    let more = if n > preview {
        format!(" (showing first {preview})")
    } else {
        String::new()
    };
    format!(
        "<div class=\"gometry-geom-array\"><div>{n} geometries{more}</div>\
         <div style=\"display:flex;flex-wrap:wrap\">{cells}</div></div>"
    )
}

fn missing_svg() -> String {
    "<svg xmlns=\"http://www.w3.org/2000/svg\" class=\"gometry-geom\" viewBox=\"0 0 120 80\">\
     <rect x=\"1\" y=\"1\" width=\"118\" height=\"78\" fill=\"#f7f7f7\" stroke=\"#d0d0d0\"/>\
     <text x=\"60\" y=\"44\" text-anchor=\"middle\" font-size=\"14\" fill=\"#777\">missing</text>\
     </svg>"
        .to_owned()
}

pub(crate) fn render_shape_svg(shape: &Shape) -> String {
    const EMPTY: &str = "<svg xmlns=\"http://www.w3.org/2000/svg\" class=\"gometry-geom\"/>";
    if shape.is_empty() {
        return EMPTY.to_owned();
    }
    let Some(bounds) = svg_padded_bounds(shape) else {
        return EMPTY.to_owned();
    };
    let (minx, miny, maxx, maxy) = bounds.into_tuple();
    let w = maxx - minx;
    let h = maxy - miny;
    if !w.is_finite() || !h.is_finite() || w <= 0.0 || h <= 0.0 {
        return EMPTY.to_owned();
    }
    // Map world → local pixel frame so extreme-but-finite coordinates never
    // produce infinite viewBox/stroke or NaN transforms.
    let scale = SVG_MAX_PX / w.max(h);
    if !scale.is_finite() || scale <= 0.0 {
        return EMPTY.to_owned();
    }
    let px_w = (w * scale).max(1.0);
    let px_h = (h * scale).max(1.0);
    if !px_w.is_finite() || !px_h.is_finite() {
        return EMPTY.to_owned();
    }
    let sw = 1.5_f64;
    let r = 3.0_f64;
    let valid = shape.validate().is_none();
    let stroke = if valid { "#22c55e" } else { "#ef4444" };
    let fill = if valid {
        "rgba(34,197,94,0.25)"
    } else {
        "rgba(239,68,68,0.25)"
    };
    let body = svg_elements_local(shape, stroke, fill, sw, r, minx, miny, scale);
    // Local frame already puts y increasing up after flip about px_h.
    format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" class=\"gometry-geom\" \
         width=\"{px_w:.0}\" height=\"{px_h:.0}\" viewBox=\"0 0 {px_w:.6} {px_h:.6}\" \
         preserveAspectRatio=\"xMidYMid meet\">\
         <g transform=\"matrix(1 0 0 -1 0 {px_h:.6})\">{body}</g></svg>"
    )
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
) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    for (i, p) in points.iter_coords().enumerate() {
        let (x, y) = map_xy(p.x, p.y, minx, miny, scale);
        if !x.is_finite() || !y.is_finite() {
            continue;
        }
        let cmd = if i == 0 { 'M' } else { 'L' };
        let _ = write!(s, "{cmd}{x:.6} {y:.6} ");
    }
    if close {
        s.push('Z');
    }
    s.truncate(s.trim_end().len());
    s
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
) -> String {
    let mut out = String::new();
    let circle = |p: &Point| {
        let (x, y) = map_xy(p.x, p.y, minx, miny, scale);
        format!("<circle cx=\"{x:.6}\" cy=\"{y:.6}\" r=\"{r:.6}\" fill=\"{stroke}\"/>")
    };
    let polyline = |pts: &CoordSeq| {
        format!(
            "<path d=\"{}\" fill=\"none\" stroke=\"{stroke}\" stroke-width=\"{sw:.6}\"/>",
            svg_path_points_local(pts, false, minx, miny, scale)
        )
    };
    match shape {
        Shape::Point(p) => out.push_str(&circle(p)),
        Shape::MultiPoint(points) => {
            for p in points {
                out.push_str(&circle(&p));
            }
        },
        Shape::LineString(points) => out.push_str(&polyline(points)),
        Shape::MultiLineString(lines) => {
            for line in lines {
                out.push_str(&polyline(line));
            }
        },
        Shape::Polygon(polygon) => {
            use std::fmt::Write as _;
            let mut d = svg_path_points_local(&polygon.shell, true, minx, miny, scale);
            for hole in polygon.holes.iter() {
                d.push(' ');
                d.push_str(&svg_path_points_local(hole, true, minx, miny, scale));
            }
            let _ = write!(
                out,
                "<path fill-rule=\"evenodd\" d=\"{d}\" fill=\"{fill}\" stroke=\"{stroke}\" stroke-width=\"{sw:.6}\"/>"
            );
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                out.push_str(&svg_elements_local(
                    &Shape::Polygon(polygon.clone()),
                    stroke,
                    fill,
                    sw,
                    r,
                    minx,
                    miny,
                    scale,
                ));
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                out.push_str(&svg_elements_local(
                    geometry, stroke, fill, sw, r, minx, miny, scale,
                ));
            }
        },
        Shape::Empty(..) => {},
    }
    out
}
