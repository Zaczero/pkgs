---
description: gometry licensing, bundled CRS resources, and third-party notices.
---

# License

gometry is distributed under the **Apache-2.0 OR MIT** dual license. You
may use it under the terms of either license, at your option.

```toml title="configuration: package license metadata"
license = "Apache-2.0 OR MIT"
```

The release package includes the complete `LICENSE-APACHE.md` and
`LICENSE-MIT.md` texts. Keep the applicable license text and notices when you
redistribute gometry or an artifact that bundles it.

## Third-party notices

The authoritative third-party inventory is `LICENSE-THIRD-PARTY.md`, shipped in
release packages with the license texts. It records the exact components,
versions, and license notices present in each release artifact.

## Bundled CRS resources

Release wheels bundle the PROJ CRS authority backend and the data resources that
ship with it, so a core installation does not require a system PROJ shared
library. PROJ and any transformation grid or other data resource retain their
upstream license terms. If you add or redistribute caller-supplied grid files,
check the terms from the provider of each file.

Optional integrations such as PyArrow, pandas, Polars, GeoPandas, and lonboard
remain separately licensed by their respective projects. Installing an extra
does not change gometry's license.

## See also

- [Compatibility](compatibility.md) — supported runtimes and optional boundaries.
- [Installation](../get-started/installation.md) — core and optional installs.
- [GitHub repository](https://github.com/Zaczero/pkgs/tree/main/gometry) — source
  distribution and release files.
