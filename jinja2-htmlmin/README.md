# jinja2-htmlmin

Minify HTML template **source** before Jinja compiles it. For Python 3.10+
applications using source-backed Jinja loaders.

## Why Use This?

Source minification removes selected HTML comments, spacing, and attribute
syntax before compilation. Repeated renders of Jinja's cached templates reuse
that work.

## Installation

```sh
pip install jinja2-htmlmin
```

## Quick Start

Run this script after installation; it creates its own template:

```python
from jinja2 import DictLoader, Environment
from jinja2_htmlmin import minify_loader

loader = DictLoader({
    "index.html": "<!-- greeting --><p>{{ title }}</p>",
})
env = Environment(
    loader=minify_loader(loader, remove_comments=True),
    autoescape=True,
)
html = env.get_template("index.html").render(title="<Hello>")
assert html == "<p>&lt;Hello&gt;</p>"
print(html)
```

Next, replace `DictLoader` with `FileSystemLoader` pointing at your template
directory. Keep Jinja autoescaping configured for your application;
minification does not escape or sanitize HTML.

## How It Works

### Loader and Cache Behavior

`minify_loader` mutates the supplied loader's `get_source` method and returns
that same object. Configure it before loading templates. `DictLoader`,
`FileSystemLoader`, `PackageLoader`, and `FunctionLoader` use this source
boundary. Jinja's `ChoiceLoader` and `PrefixLoader` delegate loading directly
to child loaders: wrap each source-backed child, rather than the composite.
For example:

```python
from jinja2 import DictLoader, Environment, PrefixLoader
from jinja2_htmlmin import minify_loader

loader = PrefixLoader({
    "pages": minify_loader(
        DictLoader({"index.html": "<!-- removed --><p>{{ name }}</p>"}),
        remove_comments=True,
    ),
})
env = Environment(loader=loader, autoescape=True)
assert env.get_template("pages/index.html").render(name="Ada") == "<p>Ada</p>"
```

Precompiled `ModuleLoader` templates bypass `get_source` and are not minified
by this wrapper. `Environment.from_string` also bypasses loaders.

Each source load creates an independent HTML parser, so shared loaders do not
share per-document parser state. Jinja owns template and bytecode caching.
A cached template can render repeatedly without another source minification;
cache misses, disabled caches, eviction, or auto-reload after a source change
can repeat it. The original source filename and freshness callback are preserved.

## Minification Options and Limits

Jinja's lexer protects expressions, statements, comments, raw blocks, and
configured custom delimiters while htmlmin2 minifies the surrounding HTML.
Raw-block content is preserved. Values inserted during rendering are not
subsequently minified.

Pass htmlmin2 options as keyword arguments to `minify_loader`. The
[public function and typed option names](jinja2_htmlmin/__init__.py) are the
package reference; [htmlmin2's Minifier reference](https://htmlmin.readthedocs.io/en/latest/reference.html)
describes their defaults and HTML behavior:

- `remove_comments`, `remove_empty_space`, and `remove_all_empty_space`
  control comment and whitespace removal.
- `reduce_empty_attributes`, `reduce_boolean_attributes`, and
  `remove_optional_attribute_quotes` control attribute reduction.
- `convert_charrefs` controls character-reference conversion.
- `keep_pre`, `pre_tags`, and `pre_attr` control whitespace preservation.
  `pre_tags` is captured when the wrapper is configured.
- `cls` selects an `htmlmin.parser.HTMLMinParser` subclass; each source load
  constructs a new instance with the configured options.

Whitespace removal can change inline text spacing, and comment removal can
remove application-specific markers. Choose options for your templates and
check rendered output. HTML minification is not a promise of arbitrary DOM
equivalence. Source-loader, Jinja lexer/compiler, and htmlmin2 errors propagate;
minifier construction and parsing happen when source is loaded.

## Support and Reference

See the [public function reference](jinja2_htmlmin/__init__.py),
[Jinja loader documentation](https://jinja.palletsprojects.com/en/stable/api/#loaders),
and [issue tracker](https://github.com/Zaczero/pkgs/issues).
The package uses the [Zero-Clause BSD license](https://choosealicense.com/licenses/0bsd/).
