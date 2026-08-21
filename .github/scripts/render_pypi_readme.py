import os
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

# Targets that already resolve wherever the description is rendered.
ABSOLUTE_TARGET = ('http://', 'https://', 'mailto:', '#', '/')

MARKDOWN_TARGET = re.compile(r'(!?\[[^\]]*\]\()([^)]+)(\))')
# PyPI keeps HTML blocks when it renders the description, so a relative `src`
# or `href` there 404s exactly as a relative markdown target does. Both header
# blocks are centered HTML, which GitHub-flavored Markdown cannot express, so
# this is the only form the logo and badge markup can take.
HTML_TARGET = re.compile(r'(<[a-zA-Z][^>]*?\s(?:src|href)\s*=\s*)(["\'])([^"\']*)\2')
# A fenced block shows markup rather than using it. Rewriting a path inside one
# corrupts the example it exists to document.
CODE_FENCE = re.compile(r'^(?P<fence>```+|~~~+)[^\n]*$.*?(?:^(?P=fence)[^\n]*$|\Z)', re.M | re.S)


def absolutize(target: str, base: str) -> str:
    return target if target.startswith(ABSOLUTE_TARGET) else urljoin(base, target)


def render(text: str, base: str) -> str:
    def rewrite(chunk: str) -> str:
        chunk = MARKDOWN_TARGET.sub(lambda m: f'{m[1]}{absolutize(m[2], base)}{m[3]}', chunk)
        return HTML_TARGET.sub(lambda m: f'{m[1]}{m[2]}{absolutize(m[3], base)}{m[2]}', chunk)

    rendered = []
    position = 0
    for fence in CODE_FENCE.finditer(text):
        rendered.append(rewrite(text[position : fence.start()]))
        rendered.append(fence.group())
        position = fence.end()
    rendered.append(rewrite(text[position:]))
    return ''.join(rendered)


def main() -> None:
    package = Path(sys.argv[1])
    ref = sys.argv[2] if len(sys.argv) > 2 else 'main'
    repo = os.environ.get('GITHUB_REPOSITORY', 'Zaczero/pkgs')
    readme = package / 'README.md'
    base = f'https://raw.githubusercontent.com/{repo}/{ref}/{package.as_posix()}/'
    readme.write_text(render(readme.read_text(), base))


if __name__ == '__main__':
    main()
