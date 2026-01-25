from __future__ import annotations

import re
from functools import lru_cache

from starlette.datastructures import Headers

TYPE_CHECKING = False
if TYPE_CHECKING:
    from starlette.types import Message

_accept_encoding_re = re.compile(r'[a-z]{2,8}')


@lru_cache(maxsize=128)
def parse_accept_encoding(accept_encoding: str) -> frozenset[str]:
    """Parse the accept encoding header and return a set of supported encodings.

    >>> _parse_accept_encoding('br;q=1.0, gzip;q=0.8, *;q=0.1')
    {'br', 'gzip'}
    """
    return frozenset(_accept_encoding_re.findall(accept_encoding))


# Based on
# - https://github.com/h5bp/server-configs-nginx/blob/main/h5bp/web_performance/compression.conf#L38
# - https://developers.cloudflare.com/speed/optimization/content/compression/
_compress_content_types: set[str] = {
    'application/atom+xml',
    'application/connect+json',
    'application/connect+proto',
    'application/eot',
    'application/font-sfnt',
    'application/font-woff',
    'application/font',
    'application/geo+json',
    'application/gpx+xml',
    'application/graphql+json',
    'application/javascript-binast',
    'application/javascript',
    'application/json',
    'application/ld+json',
    'application/manifest+json',
    'application/opentype',
    'application/otf',
    'application/proto',
    'application/protobuf',
    'application/rdf+xml',
    'application/rss+xml',
    'application/truetype',
    'application/ttf',
    'application/vnd.api+json',
    'application/vnd.google.protobuf',
    'application/vnd.mapbox-vector-tile',
    'application/vnd.ms-fontobject',
    'application/wasm',
    'application/x-google-protobuf',
    'application/x-httpd-cgi',
    'application/x-javascript',
    'application/x-opentype',
    'application/x-otf',
    'application/x-perl',
    'application/x-protobuf',
    'application/x-ttf',
    'application/x-web-app-manifest+json',
    'application/xhtml+xml',
    'application/xml',
    'font/eot',
    'font/otf',
    'font/ttf',
    'font/x-woff',
    'image/bmp',
    'image/svg+xml',
    'image/vnd.microsoft.icon',
    'image/x-icon',
    'multipart/bag',
    'multipart/mixed',
    'text/cache-manifest',
    'text/calendar',
    'text/css',
    'text/html',
    'text/javascript',
    'text/js',
    'text/markdown',
    'text/plain',
    'text/richtext',
    'text/vcard',
    'text/vnd.rim.location.xloc',
    'text/vtt',
    'text/x-component',
    'text/x-cross-domain-policy',
    'text/x-java-source',
    'text/x-markdown',
    'text/x-script',
    'text/xml',
}


def add_compress_type(content_type: str) -> None:
    """Add a new content-type to be compressed."""
    _compress_content_types.add(content_type)


def remove_compress_type(content_type: str) -> None:
    """Remove a content-type from being compressed."""
    _compress_content_types.discard(content_type)


def is_start_message_satisfied(message: Message) -> bool:
    """Check if response should be compressed based on the start message."""
    headers = Headers(raw=message['headers'])

    # must not already be compressed
    for content_encoding in headers.getlist('Content-Encoding'):
        for v in content_encoding.split(','):
            v = v.strip()
            if v and v.lower() != 'identity':
                return False

    # content-type header must be present
    content_type = headers.get('Content-Type')
    if not content_type:
        return False

    # must be a compressible content-type
    basic_content_type = content_type.split(';', maxsplit=1)[0].strip()
    return basic_content_type in _compress_content_types
