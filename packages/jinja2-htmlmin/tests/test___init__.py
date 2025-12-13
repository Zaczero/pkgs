from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from jinja2_htmlmin import minify_loader


def test_minify_loader():
    env = Environment(
        loader=minify_loader(
            FileSystemLoader("tests/templates"),
            remove_comments=True,
            remove_empty_space=True,
            remove_all_empty_space=True,
            reduce_boolean_attributes=True,
        ),
        autoescape=True,
    )

    rendered = env.get_template("index.html.jinja").render(
        title="<Test Page>",
        content="<strong>Bold content</strong>",
        items=[
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
        ],
        is_active=True,
    )

    try:
        assert rendered == Path("tests/templates/index.html").read_text()
    except Exception:
        Path("tests/templates/.index.html").write_text(rendered)
        raise
