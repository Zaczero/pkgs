"""Publish Markdown alternatives from the rendered documentation content."""

from __future__ import annotations

from html import escape
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from markdownify import MarkdownConverter

if TYPE_CHECKING:
    from bs4 import Tag
    from jinja2 import Environment
    from properdocs.config.defaults import ProperDocsConfig
    from properdocs.structure.files import Files
    from properdocs.structure.pages import Page


def _markdown_path(page: Page) -> str:
    return f'{page.url.rstrip("/") or "index"}.md'


def _code_language(element: Tag) -> str:
    for node in (element, element.parent, element.find('code')):
        if node is not None:
            for name in node.get('class', []):
                if name.startswith('language-'):
                    return name.removeprefix('language-')
    return ''


class _DocumentationConverter(MarkdownConverter):
    def convert_code(self, el, text, parent_tags):
        if 'td' in parent_tags or 'th' in parent_tags:
            text = text.replace('|', r'\|')
        return super().convert_code(el, text, parent_tags)


def on_env(env: Environment, *, config: ProperDocsConfig, files: Files) -> Environment:
    index = [
        f'# {config.site_name}',
        '',
        f'> {config.site_description}',
        '',
        '## Documentation',
        '',
    ]
    for file in files.documentation_pages():
        page = file.page
        if page is None or not page.content:
            continue
        destination = _markdown_path(page)
        description = page.meta.get('description', '')
        index.append(
            f'- [{page.title}]({urljoin(config.site_url, destination)}): {description}'
        )
    Path(config.site_dir, 'llms.txt').write_text(
        '\n'.join(index) + '\n', encoding='utf-8'
    )
    return env


def on_post_page(output: str, *, page: Page, config: ProperDocsConfig) -> str:
    soup = BeautifulSoup(output, 'html.parser')
    article = soup.select_one('article.md-content__inner')
    if article is None:
        raise ValueError(f'Missing documentation article: {page.url}')
    for tabs in article.select('.tabbed-set'):
        labels = tabs.select(':scope > .tabbed-labels > label')
        blocks = tabs.select(':scope > .tabbed-content > .tabbed-block')
        for label, block in zip(labels, blocks, strict=True):
            title = soup.new_tag('p')
            strong = soup.new_tag('strong')
            strong.string = label.get_text(' ', strip=True)
            title.append(strong)
            block.insert(0, title)
        for labels_container in tabs.select(':scope > .tabbed-labels'):
            labels_container.decompose()
    for element in article.select(
        '.headerlink, .doc-symbol, .md-content__button, script, style'
    ):
        element.decompose()
    for element in article.select('[href], [src]'):
        for attribute in ('href', 'src'):
            if element.has_attr(attribute):
                element[attribute] = urljoin(page.canonical_url, element[attribute])
    text = _DocumentationConverter(
        heading_style='ATX',
        bullets='-',
        code_language_callback=_code_language,
    ).convert_soup(article)
    destination = Path(config.site_dir) / _markdown_path(page)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        f'Source: {page.canonical_url}\n\n{text.strip()}\n', encoding='utf-8'
    )
    markdown_url = escape(urljoin(config.site_url, _markdown_path(page)), quote=True)
    index_url = escape(urljoin(config.site_url, 'llms.txt'), quote=True)
    links = (
        f'<link rel="alternate" type="text/markdown" href="{markdown_url}">\n'
        f'<link rel="describedby" type="text/plain" href="{index_url}">\n'
    )
    return output.replace('</head>', links + '</head>', 1)
