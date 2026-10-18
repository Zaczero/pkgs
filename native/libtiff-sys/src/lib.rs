// Native build provider for the statically linked libtiff archive.

// libtiff's archive references zlib; this keeps the bundled static zlib linked.
use libz_sys as _;
