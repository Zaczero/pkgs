import os
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

package = Path(sys.argv[1])
ref = sys.argv[2] if len(sys.argv) > 2 else 'main'
repo = os.environ.get('GITHUB_REPOSITORY', 'Zaczero/pkgs')
readme = package / 'README.md'
base = f'https://raw.githubusercontent.com/{repo}/{ref}/{package.as_posix()}/'
text = re.sub(
    r'(!?\[[^\]]*\]\()([^)]+)(\))',
    lambda match: (
        f'{match.group(1)}'
        f'{match.group(2) if match.group(2).startswith(("http://", "https://", "mailto:", "#", "/")) else urljoin(base, match.group(2))}'
        f'{match.group(3)}'
    ),
    readme.read_text(),
)
readme.write_text(text)
