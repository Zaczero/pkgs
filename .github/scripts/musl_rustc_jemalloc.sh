#!/bin/sh
# The musl-hosted rustc allocates through musl's malloc rather than jemalloc,
# which made gometry's release build about 17% slower with identical output.
# Preload jemalloc into compiler invocations only, not the Python that builds and tests wheels.
set -eu
rustc -vV | grep -q '^host: .*-musl' || exit 0
apk add --no-cache jemalloc
proxy=$(command -v rustc)
compiler=$(rustup which rustc)
# The proxy is a symbolic link to rustup: replace the link, never write through it.
rm "$proxy"
printf '#!/bin/sh\nLD_PRELOAD=/usr/lib/libjemalloc.so.2 exec %s "$@"\n' "$compiler" > "$proxy"
chmod +x "$proxy"
MALLOC_CONF=stats_print:true rustc -vV 2>&1 | grep -q 'Begin jemalloc statistics'
