from pathlib import Path

from jinja2 import DictLoader, Environment, FileSystemLoader

import jinja2_htmlmin
from jinja2_htmlmin import _restore_placeholders, minify_loader


def render_template(source: str, **context: object) -> str:
    return render_template_with_environment(Environment(autoescape=True), source, **context)


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


def test_minify_loader():
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
    except Exception:
        Path('tests/templates/.index.html').write_text(rendered)
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
        '<p>{{\n value\n}}</p>'
        '{%\n if ok\n%}<b>yes</b>{% endif %}'
        '{#\n hidden\n#}',
        value='content',
        ok=True,
    )

    assert rendered == '<p>content</p><b>yes</b>'


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
