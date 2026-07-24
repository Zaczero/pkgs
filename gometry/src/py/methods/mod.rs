//! Unary geometry/array method implementations (the `Geometry`/`GeometryArray`
//! receiver-op families), split by domain and sharing the scalar/array dedup
//! macros in `method_macro`.

#[macro_use]
mod unary_method_macro;
mod unary_constructive_methods;
mod unary_io_methods;
mod unary_linref_methods;
mod unary_ordinate_methods;
mod unary_transform_methods;
