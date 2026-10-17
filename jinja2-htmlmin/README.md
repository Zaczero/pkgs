# jinja2-htmlmin

Minify HTML template **source** before Jinja compiles it. For Python 3.11+
applications using source-backed Jinja loaders.

<a id="why-use-this"></a>

Source minification removes selected HTML comments, spacing, and attribute
syntax before compilation. Repeated renders of Jinja's cached templates reuse
that work.

## Installation

```sh
pip install jinja2-htmlmin
```

## Quick Start

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

<a id="how-it-works"></a>

## Loader and Cache Behavior

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

Cached templates render without repeating minification. Reloading a changed
template minifies its new source.

## Minification Options and Limits

Jinja's lexer protects expressions, statements, comments, raw blocks, and
configured custom delimiters while htmlmin2 minifies the surrounding HTML.
Raw-block content is preserved. Values inserted during rendering are not
subsequently minified.

Pass htmlmin2 options as keyword arguments to `minify_loader`. The
[function reference](jinja2_htmlmin/__init__.py) lists the options;
[htmlmin2's Minifier reference](https://htmlmin.readthedocs.io/en/latest/reference.html)
describes their defaults and behavior:

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
check rendered output.

## Support and Reference

See the [public function reference](jinja2_htmlmin/__init__.py) and
[Jinja loader documentation](https://jinja.palletsprojects.com/en/stable/api/#loaders).
The package uses the [Zero-Clause BSD license](https://choosealicense.com/licenses/0bsd/).

[Issues and questions](https://github.com/Zaczero/pkgs/issues) ·
[Private security reports](https://github.com/Zaczero/pkgs/security/advisories/new) ·
[Premium support](https://monicz.dev/#get-in-touch)
