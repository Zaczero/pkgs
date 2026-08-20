# Low-level bindings for PROJ v9.6.x

This vendored tree is a path fork of `proj-sys` 0.27.0. Its fork-specific
change replaces upstream system/dynamic libtiff and pkg-config linking with
statically linked vendored libtiff and zlib. PROJ 9.6.2 and
`network = ["tiff"]` are upstream proj-sys features.

The libtiff source is the full upstream `v4.7.2` tree at commit
`d01a94be176f5f6a87f7ee1c0b32e65416aa2b4d`, recorded in
`../libtiff-sys/libtiff.pin`. Initialize it before building from a fresh
checkout:

```sh
git submodule update --init gometry/native/libtiff-sys/libtiff
```

Builds do not fetch source or transformation grids from the network. The
`tiff` feature enables user-supplied PROJ GeoTIFF transformation grids; it is
not a gometry raster API.

**This is a
[`*-sys`](https://doc.rust-lang.org/cargo/reference/build-scripts.html#-sys-packages)
crate; you shouldn't use its API directly.** See the
[`proj`](https://github.com/georust/proj) crate for general use.

A guide to PROJ functions can be found here:
https://proj.org/development/reference/functions.html. 

By default, the crate will search for an acceptable existing `libproj`
installation on your system using
[pkg-config](https://www.freedesktop.org/wiki/Software/pkg-config/). 

If an acceptable installation is not found, proj-sys will attempt to build
libproj from source bundled in the crate.

## Features

- `bundled_proj` - forces building libproj from source even if an acceptable
  version could be found on your system.  Note that SQLite3 must be
  present on your system if you wish to use this feature, and that it builds
  `libproj` **without** its native network functionality; you will have to
  implement your own set of callbacks if you wish to make use of them (see the
[`proj`](https://crates.io/crates/proj) crate for an example).
- `tiff` - enables PROJ TIFF support and links the path dependency's statically
  bundled libtiff and zlib. No system libtiff is used, and builds do not fetch
  source or transformation grids from the network.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
