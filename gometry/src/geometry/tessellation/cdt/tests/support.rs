/// The exact circumcenter rounded to nearest. `None` means that the exact
/// center is outside the finite binary64 range; the refinement caller makes
/// the deliberate midpoint choice for that numerical case. Every other error
/// remains an error, including resource failures and mesh invariants.
#[cfg(test)]
fn circumcenter(a: XY, b: XY, c: XY) -> Result<Option<XY>> {
    exact::circumcenter(a, b, c)?.finite_rounding()
}
#[cfg(test)]
fn longest_edge_midpoint(a: XY, b: XY, c: XY) -> XY {
    let length = |p: XY, q: XY| (p.x - q.x).hypot(p.y - q.y);
    let (p, q) = if length(a, b) >= length(b, c) && length(a, b) >= length(c, a) {
        (a, b)
    } else if length(b, c) >= length(c, a) {
        (b, c)
    } else {
        (c, a)
    };
    XY::new(f64::midpoint(p.x, q.x), f64::midpoint(p.y, q.y))
}
/// The constraint flags mark exactly the constrained vertex pairs, on both
/// halves of each edge.
fn assert_flags_match(mesh: &ConstrainedMesh, edges: &[[usize; 2]]) {
    for edge in mesh.mesh.edge_ids() {
        if !mesh.mesh.alive(edge.face()) {
            continue;
        }
        let (start, end) = mesh.mesh.edge_vertices(edge);
        let pair = ordered(start, end);
        let expected = edges.iter().any(|&[u, v]| ordered(u, v) == pair);
        assert_eq!(
            mesh.constrained[edge.index()],
            expected,
            "slot {} pair {pair:?}",
            edge.index()
        );
    }
}
