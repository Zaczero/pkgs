#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! SVG rendering of geometries for Jupyter/`_repr_svg_`. Pure geometry →
//! string; no `PyO3`. The Y axis is flipped so geographic north points up.

use crate::geometry::{self, Bounds, CoordSeq, Coordinates, Point, Shape};

/// Pixel budget for the longest side of a rendered SVG.
const SVG_MAX_PX: f64 = 300.0;

/// Padded bounds `(minx, miny, maxx, maxy)` with ~5% margin. Degenerate
/// (point / zero-extent) inputs are widened so the `viewBox` is never empty.
fn svg_padded_bounds(shape: &Shape) -> Option<Bounds> {
    let b = shape.bounds()?;
    let (mut minx, mut miny, mut maxx, mut maxy) = (b.minx(), b.miny(), b.maxx(), b.maxy());
    let mut w = maxx - minx;
    let mut h = maxy - miny;
    if w <= 0.0 {
        let pad = if h > 0.0 { h * 0.5 } else { 1.0 };
        minx -= pad;
        maxx += pad;
        w = maxx - minx;
    }
    if h <= 0.0 {
        let pad = if w > 0.0 { w * 0.5 } else { 1.0 };
        miny -= pad;
        maxy += pad;
        h = maxy - miny;
    }
    Some(Bounds::new_unchecked(
        minx - w * 0.05,
        miny - h * 0.05,
        maxx + w * 0.05,
        maxy + h * 0.05,
    ))
}

/// Build an SVG path-data string from a point sequence (`M`/`L` commands,
/// optionally closed with `Z`).
fn svg_path_points<C: Coordinates + ?Sized>(points: &C, close: bool) -> String {
    use std::fmt::Write as _;
    let mut s = String::new();
    for (i, p) in points.iter_coords().enumerate() {
        let cmd = if i == 0 { 'M' } else { 'L' };
        // SVG reprs intentionally use fixed precision, not zmij's shortest round-trip writer.
        let _ = write!(s, "{cmd}{:.6} {:.6} ", p.x, p.y);
    }
    if close {
        s.push('Z');
    }
    s.truncate(s.trim_end().len());
    s
}

/// Render a single polygon (shell + holes) as one even-odd `<path>`.
fn svg_polygon(polygon: &geometry::Polygon, stroke: &str, fill: &str, sw: f64) -> String {
    let mut d = svg_path_points(&polygon.shell, true);
    for hole in polygon.holes.iter() {
        d.push(' ');
        d.push_str(&svg_path_points(hole, true));
    }
    format!(
        "<path fill-rule=\"evenodd\" d=\"{d}\" fill=\"{fill}\" stroke=\"{stroke}\" stroke-width=\"{sw:.6}\"/>"
    )
}

/// Render the inner SVG elements (no `<svg>` wrapper) for a shape.
fn svg_elements(shape: &Shape, stroke: &str, fill: &str, sw: f64, r: f64) -> String {
    let mut out = String::new();
    let circle = |p: &Point| {
        format!(
            "<circle cx=\"{:.6}\" cy=\"{:.6}\" r=\"{r:.6}\" fill=\"{stroke}\"/>",
            p.x, p.y
        )
    };
    let polyline = |pts: &CoordSeq| {
        format!(
            "<path d=\"{}\" fill=\"none\" stroke=\"{stroke}\" stroke-width=\"{sw:.6}\"/>",
            svg_path_points(pts, false)
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
        Shape::Polygon(polygon) => out.push_str(&svg_polygon(polygon, stroke, fill, sw)),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                out.push_str(&svg_polygon(polygon, stroke, fill, sw));
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                out.push_str(&svg_elements(geometry, stroke, fill, sw, r));
            }
        },
        Shape::Empty(..) => {},
    }
    out
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
    let scale = SVG_MAX_PX / w.max(h);
    let px_w = (w * scale).max(1.0);
    let px_h = (h * scale).max(1.0);
    let unit = w.max(h);
    let per_px = unit / SVG_MAX_PX;
    let sw = per_px * 1.5;
    let r = per_px * 3.0;
    let valid = shape.validate().is_none();
    let stroke = if valid { "#22c55e" } else { "#ef4444" };
    let fill = if valid {
        "rgba(34,197,94,0.25)"
    } else {
        "rgba(239,68,68,0.25)"
    };
    let body = svg_elements(shape, stroke, fill, sw, r);
    // The flip mirrors y about the bounds' vertical center (y' = miny+maxy-y),
    // keeping content inside [miny, maxy] — so the viewBox starts at miny.
    format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" class=\"gometry-geom\" \
         width=\"{px_w:.0}\" height=\"{px_h:.0}\" viewBox=\"{minx:.6} {miny:.6} {w:.6} {h:.6}\" \
         preserveAspectRatio=\"xMidYMid meet\">\
         <g transform=\"matrix(1 0 0 -1 0 {flip:.6})\">{body}</g></svg>",
        flip = miny + maxy,
    )
}
