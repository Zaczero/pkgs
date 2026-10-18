import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from htmlmin.parser import HTMLMinParser
from jinja2 import (
    ChoiceLoader,
    DictLoader,
    Environment,
    FileSystemLoader,
    ModuleLoader,
    PrefixLoader,
    TemplateSyntaxError,
)

import jinja2_htmlmin
from jinja2_htmlmin import _restore_placeholders, minify_loader


def render_template(source: str, **context: object) -> str:
    return render_template_with_environment(
        Environment(autoescape=True), source, **context
    )


def render_template_with_environment(
    env: Environment,
    source: str,
    **context: object,
) -> str:
    env.loader = minify_loader(
        DictLoader({'template.html': source}),
        remove_empty_space=True,
        remove_all_empty_space=True,
    )

    return env.get_template('template.html').render(**context)


def test_minify_loader(tmp_path):
    env = Environment(
        loader=minify_loader(
            FileSystemLoader('tests/templates'),
            remove_comments=True,
            remove_empty_space=True,
            remove_all_empty_space=True,
            reduce_boolean_attributes=True,
        ),
        autoescape=True,
    )

    rendered = env.get_template('index.html.jinja').render(
        title='<Test Page>',
        content='<strong>Bold content</strong>',
        items=[
            {'id': 1, 'name': 'Item 1'},
            {'id': 2, 'name': 'Item 2'},
        ],
        is_active=True,
    )

    try:
        assert rendered == Path('tests/templates/index.html').read_text()
    except AssertionError:
        (tmp_path / 'index.html').write_text(rendered)
        raise


def test_literal_legacy_placeholder_is_preserved():
    rendered = render_template(
        '<p>{{ value }}</p><span>__jinja2_htmlmin 00000__</span>',
        value='content',
    )

    assert rendered == '<p>content</p><span>__jinja2_htmlmin 00000__</span>'


def test_out_of_range_legacy_placeholder_does_not_crash():
    rendered = render_template('<span>__jinja2_htmlmin 99999__</span>')

    assert rendered == '<span>__jinja2_htmlmin 99999__</span>'


def test_literal_current_placeholder_prefix_collision_is_preserved(monkeypatch):
    tokens = iter([0x12345678, 0x87654321])
    monkeypatch.setattr(jinja2_htmlmin, 'getrandbits', lambda _: next(tokens))

    rendered = render_template(
        '<p>{{ value }}</p><span>j2h12345678`00000z</span>',
        value='content',
    )

    assert rendered == '<p>content</p><span>j2h12345678`00000z</span>'


def test_entity_decoded_current_placeholder_collision_is_preserved(monkeypatch):
    tokens = iter([0x12345678, 0x87654321])
    monkeypatch.setattr(jinja2_htmlmin, 'getrandbits', lambda _: next(tokens))

    rendered = render_template(
        '<p>{{ value }}</p><a href="j2h12345678&#96;00000z">link</a>',
        value='content',
    )

    assert rendered == '<p>content</p><a href="j2h12345678`00000z">link</a>'


def test_placeholder_index_is_not_limited_to_five_digits():
    lookup = [''] * 100_001
    lookup[100_000] = '{{ value }}'

    assert (
        _restore_placeholders(
            '<p>j2h12345678`100000z</p>',
            'j2h12345678`',
            lookup,
        )
        == '<p>{{ value }}</p>'
    )


def test_multiline_jinja_syntax_is_preserved():
    rendered = render_template(
        '<p>{{\n value\n}}</p>{%\n if ok\n%}<b>yes</b>{% endif %}{#\n hidden\n#}',
        value='content',
        ok=True,
    )

    assert rendered == '<p>content</p><b>yes</b>'


@pytest.mark.parametrize(
    'source',
    [
        pytest.param('<p>kept</p>{{ value', id='unfinished-variable'),
        pytest.param('<p>kept</p>{% if value', id='unfinished-block'),
        pytest.param('<p>kept</p>{{', id='empty-variable'),
        pytest.param('<p>kept</p>{%', id='empty-block'),
    ],
)
def test_unfinished_jinja_tags_reach_the_parser(source):
    loader = minify_loader(DictLoader({'broken.html': source}))
    environment = Environment(loader=loader, autoescape=True)

    with pytest.raises(TemplateSyntaxError) as caught:
        environment.get_template('broken.html')

    assert caught.value.name == 'broken.html'
    assert caught.value.lineno == 1


def test_unfinished_custom_delimiter_preserves_filesystem_error_context(tmp_path):
    template = tmp_path / 'broken.html'
    template.write_text('heading\n[[ value')
    plain = Environment(
        loader=FileSystemLoader(tmp_path),
        variable_start_string='[[',
        variable_end_string=']]',
        autoescape=True,
    )
    wrapped = Environment(
        loader=minify_loader(FileSystemLoader(tmp_path)),
        variable_start_string='[[',
        variable_end_string=']]',
        autoescape=True,
    )

    with pytest.raises(TemplateSyntaxError) as expected:
        plain.get_template('broken.html')
    with pytest.raises(TemplateSyntaxError) as caught:
        wrapped.get_template('broken.html')

    assert caught.value.message == expected.value.message
    assert caught.value.name == expected.value.name == 'broken.html'
    assert caught.value.filename == expected.value.filename == str(template)
    assert caught.value.lineno == expected.value.lineno == 2


def test_delimiter_text_inside_jinja_strings_is_preserved():
    rendered = render_template(
        '<p>{{ "a }} b"|length }}</p>'
        '<p>{% set x = "%}" %}{{ x }}</p>'
        '<a href=\'{{ "x }} y" }}\'>link</a>',
    )

    assert rendered == '<p>6</p><p>%}</p><a href="x }} y">link</a>'


def test_raw_block_content_is_not_minified():
    rendered = render_template('{% raw %}<b>  {{ literal }}  </b>{% endraw %}')

    assert rendered == '<b>  {{ literal }}  </b>'


def test_custom_variable_delimiters_are_preserved():
    env = Environment(
        variable_start_string='[[',
        variable_end_string=']]',
        autoescape=True,
    )

    rendered = render_template_with_environment(env, '<p>[[ value ]]</p>', value='ok')

    assert rendered == '<p>ok</p>'


def test_line_statement_and_comment_prefixes_are_preserved():
    env = Environment(
        line_statement_prefix='#',
        line_comment_prefix='##',
        autoescape=True,
    )

    rendered = render_template_with_environment(
        env,
        '# if ok\n<p>{{ value }}</p>\n# endif\n## hidden',
        value='ok',
        ok=True,
    )

    assert rendered == '<p>ok</p>\n'


def test_dynamic_tag_name_is_preserved():
    rendered = render_template(
        '<{{ tag }} class="x">{{ value }}</{{ tag }}>',
        tag='section',
        value='content',
    )

    assert rendered == '<section class=x>content</section>'


def test_quoted_jinja_attribute_value_keeps_quotes():
    rendered = render_template(
        '<input value="{{ value }}">',
        value='hello world',
    )

    assert rendered == '<input value="hello world">'


def test_jinja_attribute_splice_is_preserved():
    rendered = render_template(
        '<input {{ attrs }}>',
        attrs='disabled checked',
    )

    assert rendered == '<input disabled checked>'


def test_static_boolean_attribute_value_is_still_reduced():
    env = Environment(
        loader=minify_loader(
            DictLoader({'template.html': '<input disabled="disabled">'}),
            remove_empty_space=True,
            remove_all_empty_space=True,
            reduce_boolean_attributes=True,
        ),
        autoescape=True,
    )

    rendered = env.get_template('template.html').render()

    assert rendered == '<input disabled>'


def test_shared_loader_keeps_concurrent_parser_state_separate():
    fed = Barrier(2, timeout=10)

    class InterleavedParser(HTMLMinParser):
        def feed(self, data):
            super().feed(data)
            fed.wait()

    loader = minify_loader(
        DictLoader({
            'one.html': '<p>  {{ value }}  </p>',
            'two.html': '<b>  {{ value }}  </b>',
        }),
        cls=InterleavedParser,
        remove_all_empty_space=True,
    )

    def render(name):
        env = Environment(loader=loader, autoescape=True)
        return env.get_template(name).render(value=name)

    with ThreadPoolExecutor(max_workers=2) as workers:
        one = workers.submit(render, 'one.html')
        two = workers.submit(render, 'two.html')
        assert one.result(timeout=10) == '<p> one.html </p>'
        assert two.result(timeout=10) == '<b> two.html </b>'


@pytest.mark.parametrize('composite', ['choice', 'prefix'])
def test_wrapped_source_loader_inside_composite(composite):
    source = DictLoader({'index.html': '<p>\n {{ value }}\n </p>'})
    wrapped = minify_loader(source, remove_all_empty_space=True)
    assert wrapped is source
    if composite == 'choice':
        loader = ChoiceLoader([DictLoader({}), wrapped])
        name = 'index.html'
    else:
        loader = PrefixLoader({'pages': wrapped})
        name = 'pages/index.html'
    env = Environment(loader=loader, autoescape=True)

    assert env.get_template(name).render(value='<hello>') == '<p> &lt;hello&gt; </p>'


def test_source_reload_and_cached_rendering(tmp_path):
    template = tmp_path / 'index.html'
    template.write_text('<p>\n {{ value }}\n </p>')
    env = Environment(
        loader=minify_loader(FileSystemLoader(tmp_path), remove_all_empty_space=True),
        autoescape=True,
    )
    cached = env.get_template('index.html')
    assert cached.render(value='one') == '<p> one </p>'
    assert env.get_template('index.html') is cached
    template.write_text('<b>\n {{ value }}\n </b>')
    # Force a distinct timestamp without relying on filesystem clock resolution.
    stat = template.stat()
    os.utime(template, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    assert env.get_template('index.html').render(value='two') == '<b> two </b>'


def test_precompiled_module_loader_bypasses_source_minification(tmp_path):
    source = Environment(
        loader=DictLoader({'index.html': '<!-- retained --><p>{{ value }}</p>'}),
        autoescape=True,
    )
    source.compile_templates(tmp_path, zip=None)
    env = Environment(
        loader=minify_loader(ModuleLoader(tmp_path), remove_comments=True),
        autoescape=True,
    )

    assert env.get_template('index.html').render(value='hello') == (
        '<!-- retained --><p>hello</p>'
    )
