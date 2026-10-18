"""Compile shared browser assets into the documentation site with Bun."""

from __future__ import annotations

import doctest
import json
import re
import shlex
import subprocess
from pathlib import Path

from bs4 import BeautifulSoup

_ROOT = Path(__file__).resolve().parents[2]
_MERMAID_PATH = 'assets/javascripts/mermaid-11.17.0.min.js'

_PYTHON_FENCE = re.compile(
    r'^(?P<indent>[ \t]*)(?P<fence>`{3,})python(?P<options>[^\n]*)\n'
    r'(?P<body>.*?)^(?P=indent)(?P=fence)[ \t]*(?=\n|$)',
    re.MULTILINE | re.DOTALL,
)


def on_page_markdown(markdown, *, page, config, **_kwargs):
    if not config.extra.get('browser_python'):
        return markdown
    first = True

    def wrap(match):
        nonlocal first
        options = dict(
            option.split('=', 1)
            for option in shlex.split(match['options'])
            if '=' in option
        )
        if options.get('interactive') == 'off':
            return re.sub(
                r"""[ \t]+interactive=(?:"off"|'off'|off)(?=[ \t\n])""",
                '',
                match[0],
                count=1,
            )
        if (
            options.get('exec') != 'on'
            or options.get('html') == 'true'
            or 'session' in options
        ):
            return match[0]
        attributes = 'data-python-example'
        if not page.url and first:
            attributes += ' data-python-intro'
        first = False
        indent = match['indent']
        return (
            f'{indent}<div {attributes} markdown="1">\n\n{match[0]}\n\n{indent}</div>'
        )

    return _PYTHON_FENCE.sub(wrap, markdown)


def on_page_content(html, *, config, **_kwargs):
    if not config.extra.get('browser_python') or 'language-pycon' not in html:
        return html
    soup = BeautifulSoup(html, 'html.parser')
    parser = doctest.DocTestParser()
    for block in soup.select('.language-pycon.highlight'):
        code = block.find('code')
        if code is None:
            continue
        examples = parser.get_examples(code.get_text())
        if not examples:
            continue
        wrapper = soup.new_tag('div')
        wrapper['data-python-example'] = ''
        wrapper['data-python-code'] = ''.join(
            example.source for example in examples
        ).rstrip()
        wrapper['data-python-output'] = ''.join(
            example.want for example in examples
        ).rstrip()
        block.wrap(wrapper)
    return str(soup)


def on_files(files, *, config, **_kwargs):
    if _MERMAID_PATH in config.extra_javascript:
        from mkdocs.structure.files import File

        files.append(
            File(
                path=_MERMAID_PATH,
                src_dir=str(_ROOT / 'vendor' / 'mermaid'),
                dest_dir=config.site_dir,
                use_directory_urls=config.use_directory_urls,
            )
        )
    return files


def on_post_build(config, **_kwargs):
    settings = {
        'site': str(Path(config.site_dir).resolve()),
        'siteURL': config.site_url,
        'fonts': 'assets/shared/fonts.css' in config.extra_css,
        'stylesheets': {
            path: str((Path(config.docs_dir) / path).resolve())
            for path in config.extra_css
            if (Path(config.docs_dir) / path).is_file()
        },
    }
    subprocess.run(['bun', 'install', '--frozen-lockfile'], cwd=_ROOT, check=True)
    if python := config.extra.get('browser_python'):
        if command := python.get('build'):
            subprocess.run(command, cwd=_ROOT, check=True)
        package = Path(config.config_file_path).resolve().parent
        wheels = []
        for pattern in python['wheels']:
            matches = list(package.glob(pattern))
            if len(matches) != 1:
                raise FileNotFoundError(
                    f'Expected one browser wheel matching {package / pattern}; '
                    'build the package WebAssembly wheel before building its docs.'
                )
            wheels.append(str(matches[0].resolve()))
        settings['python'] = {
            'packages': python.get('packages', []),
            'optionalPackages': python.get('optional_packages', []),
            'importHints': python.get('import_hints', {}),
            'wheels': wheels,
        }
    for command in (
        ['bun', 'run', 'typecheck'],
        ['bun', 'run', 'docs:browser', json.dumps(settings)],
    ):
        subprocess.run(command, cwd=_ROOT, check=True)
