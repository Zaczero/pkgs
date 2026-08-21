import argparse
import dataclasses
import os
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from h2corn import Config
from h2corn._cli import CliSettings, ImportSettings, build_parser, parse_cli
from h2corn._config import config_options, tcp_bind_convenience

if TYPE_CHECKING:
    from collections.abc import Callable


def test_config_fields_are_exactly_the_documented_options() -> None:
    # Constructor-only host/port sugar is an InitVar pair, not stored state.
    field_names = {config_field.name for config_field in dataclasses.fields(Config)}
    option_names = {option.name for option in config_options()}

    assert field_names == option_names


@pytest.mark.parametrize(
    ('kwargs', 'match'),
    [
        ({'backlog': 0}, 'backlog'),
        ({'workers': 0}, 'workers'),
        ({'runtime_threads': 0}, 'runtime_threads'),
        ({'loop_threads': 0}, 'loop_threads'),
        ({'user': ''}, 'user'),
        ({'group': ''}, 'group'),
        ({'umask': -1}, 'umask'),
        ({'umask': 0o1000}, 'umask'),
        ({'uds_permissions': -1}, 'uds_permissions'),
        ({'uds_permissions': 0o1000}, 'uds_permissions'),
        ({'limit_request_line': -1}, 'limit_request_line'),
        ({'limit_request_head_size': -1}, 'limit_request_head_size'),
        ({'limit_request_fields': -1}, 'limit_request_fields'),
        ({'limit_request_field_size': -1}, 'limit_request_field_size'),
        ({'timeout_graceful_shutdown': -1}, 'timeout_graceful_shutdown'),
        ({'h2_timeout_response_stall': -1}, 'h2_timeout_response_stall'),
        ({'max_concurrent_streams': 0}, 'max_concurrent_streams'),
        ({'max_concurrent_streams': -1}, 'max_concurrent_streams'),
        ({'max_concurrent_streams': 4_294_967_296}, 'max_concurrent_streams'),
        ({'h2_max_header_list_size': -1}, 'h2_max_header_list_size'),
        ({'h2_max_header_list_size': 4_294_967_296}, 'h2_max_header_list_size'),
        ({'h2_max_header_block_size': -1}, 'h2_max_header_block_size'),
        ({'h2_max_inbound_frame_size': 16_383}, 'h2_max_inbound_frame_size'),
        ({'h2_max_inbound_frame_size': 16_777_216}, 'h2_max_inbound_frame_size'),
        ({'h2_initial_stream_window_size': 65_534}, 'h2_initial_stream_window_size'),
        (
            {'h2_initial_stream_window_size': 2_147_483_648},
            'h2_initial_stream_window_size',
        ),
        (
            {'h2_initial_connection_window_size': 65_534},
            'h2_initial_connection_window_size',
        ),
        ({'max_request_body_size': -1}, 'max_request_body_size'),
        ({'max_requests': -1}, 'max_requests'),
        ({'max_requests_jitter': -1}, 'max_requests_jitter'),
        ({'limit_concurrency': -1}, 'limit_concurrency'),
        ({'limit_connections': -1}, 'limit_connections'),
        ({'timeout_handshake': -1}, 'timeout_handshake'),
        ({'websocket_max_message_size': -1}, 'websocket_max_message_size'),
        # `nan < 0` is false, so a NaN would pass a non-negative check and then
        # silently disable every `> 0` branch that reads it.
        ({'timeout_graceful_shutdown': float('nan')}, 'finite'),
        ({'timeout_graceful_shutdown': float('inf')}, 'finite'),
        ({'timeout_lifespan_startup': float('nan')}, 'finite'),
        ({'timeout_lifespan_shutdown': float('-inf')}, 'finite'),
        ({'timeout_worker_healthcheck': float('inf')}, 'finite'),
        ({'timeout_keep_alive': float('nan')}, 'finite'),
        ({'websocket_ping_interval': float('nan')}, 'finite'),
        ({'websocket_ping_timeout': float('inf')}, 'finite'),
    ],
)
def test_config_rejects_invalid_numeric_values(
    kwargs: dict[str, int | float],
    match: str,
) -> None:
    # The parametrized table intentionally dispatches runtime-invalid values
    # across heterogeneous Config fields. Keep that dynamic boundary local.
    construct_config = cast('Callable[..., Config]', Config)
    with pytest.raises(ValueError, match=match):
        construct_config(**kwargs)


# Integer fields spanning every bound family: min-only, non-negative, u32,
# frame/window, and optional mask/size. Used for exact-type ingress checks.
_INTEGER_FIELDS: tuple[str, ...] = (
    'backlog',
    'workers',
    'runtime_threads',
    'loop_threads',
    'max_requests',
    'max_requests_jitter',
    'max_concurrent_streams',
    'limit_request_head_size',
    'limit_request_line',
    'limit_request_fields',
    'limit_request_field_size',
    'h2_max_header_list_size',
    'h2_max_header_block_size',
    'h2_max_inbound_frame_size',
    'h2_initial_stream_window_size',
    'h2_initial_connection_window_size',
    'max_request_body_size',
    'limit_concurrency',
    'limit_connections',
    'websocket_max_message_size',
    'umask',
    'uds_permissions',
)

_FLOAT_FIELDS: tuple[str, ...] = (
    'timeout_lifespan_startup',
    'timeout_lifespan_shutdown',
    'timeout_worker_healthcheck',
    'timeout_handshake',
    'timeout_graceful_shutdown',
    'timeout_keep_alive',
    'timeout_request_header',
    'timeout_request_body_idle',
    'h2_timeout_response_stall',
    'websocket_ping_interval',
    'websocket_ping_timeout',
)

_BOOL_FIELDS: tuple[str, ...] = (
    'reuse_port',
    'http1',
    'access_log',
    'websocket_per_message_deflate',
    'proxy_headers',
    'date_header',
)

# Valid boundary values for each integer field (at the declared minimum or a
# representative in-range value for optional fields where None is also legal).
_INTEGER_BOUNDARIES: dict[str, int] = {
    'backlog': 1,
    'workers': 1,
    'runtime_threads': 1,
    'loop_threads': 1,
    'max_requests': 0,
    'max_requests_jitter': 0,
    'max_concurrent_streams': 1,
    'limit_request_head_size': 0,
    'limit_request_line': 0,
    'limit_request_fields': 0,
    'limit_request_field_size': 0,
    'h2_max_header_list_size': 0,
    'h2_max_header_block_size': 0,
    'h2_max_inbound_frame_size': 16_384,
    'h2_initial_stream_window_size': 65_535,
    'h2_initial_connection_window_size': 65_535,
    'max_request_body_size': 0,
    'limit_concurrency': 0,
    'limit_connections': 0,
    'websocket_max_message_size': 0,
    'umask': 0,
    'uds_permissions': 0o777,
}


@pytest.mark.parametrize('field_name', _INTEGER_FIELDS)
@pytest.mark.parametrize(
    'bad_value',
    [
        pytest.param(True, id='True'),
        pytest.param(False, id='False'),
        pytest.param(1.0, id='float-1.0'),
        pytest.param(1.5, id='float-1.5'),
        pytest.param('1', id='str-1'),
    ],
)
def test_config_integer_fields_reject_non_exact_int(
    field_name: str,
    bad_value: object,
) -> None:
    construct_config = cast('Callable[..., Config]', Config)
    with pytest.raises(TypeError, match=field_name):
        construct_config(**{field_name: bad_value})
    # Strings are env-parsed in from_mapping (TOML/env path); only non-string
    # impostors must still be rejected there.
    if not isinstance(bad_value, str):
        with pytest.raises(TypeError, match=field_name):
            Config.from_mapping({field_name: bad_value})


class _IntSubclass(int):
    """int subclass used only to prove exact-type ingress (not isinstance)."""


@pytest.mark.parametrize('field_name', _INTEGER_FIELDS)
def test_config_integer_fields_reject_int_subclass(field_name: str) -> None:
    # isinstance(value, int) is True for subclasses; type(value) is int is not.
    # A regression to isinstance would silently accept this and stay green.
    construct_config = cast('Callable[..., Config]', Config)
    bad = _IntSubclass(_INTEGER_BOUNDARIES[field_name])
    with pytest.raises(TypeError, match=field_name):
        construct_config(**{field_name: bad})
    with pytest.raises(TypeError, match=field_name):
        Config.from_mapping({field_name: bad})


@pytest.mark.parametrize('field_name', _INTEGER_FIELDS)
def test_config_integer_fields_accept_boundary(field_name: str) -> None:
    value = _INTEGER_BOUNDARIES[field_name]
    construct_config = cast('Callable[..., Config]', Config)
    values = {field_name: value}
    if field_name == 'h2_initial_connection_window_size':
        values['h2_initial_stream_window_size'] = value
    config = construct_config(**values)
    assert getattr(config, field_name) is value or getattr(config, field_name) == value
    assert type(getattr(config, field_name)) is int
    mapped = Config.from_mapping(values)
    assert getattr(mapped, field_name) == value
    assert type(getattr(mapped, field_name)) is int


@pytest.mark.parametrize('field_name', _FLOAT_FIELDS)
def test_config_float_fields_accept_exact_int_as_float(field_name: str) -> None:
    construct_config = cast('Callable[..., Config]', Config)
    config = construct_config(**{field_name: 1})
    assert getattr(config, field_name) == 1.0
    assert type(getattr(config, field_name)) is float
    mapped = Config.from_mapping({field_name: 1})
    assert getattr(mapped, field_name) == 1.0
    assert type(getattr(mapped, field_name)) is float


@pytest.mark.parametrize('field_name', _FLOAT_FIELDS)
@pytest.mark.parametrize(
    'bad_value',
    [
        pytest.param(True, id='True'),
        pytest.param(False, id='False'),
        pytest.param('1.0', id='str'),
        pytest.param(float('nan'), id='nan'),
        pytest.param(float('inf'), id='inf'),
        pytest.param(float('-inf'), id='neg-inf'),
    ],
)
def test_config_float_fields_reject_bool_str_and_nonfinite(
    field_name: str,
    bad_value: object,
) -> None:
    construct_config = cast('Callable[..., Config]', Config)
    # Non-finite values are ValueError; wrong types are TypeError.
    expected = ValueError if isinstance(bad_value, float) else TypeError
    with pytest.raises(
        expected, match=field_name if expected is TypeError else 'finite'
    ):
        construct_config(**{field_name: bad_value})


@pytest.mark.parametrize('field_name', _BOOL_FIELDS)
@pytest.mark.parametrize(
    'bad_value',
    [
        pytest.param(0, id='int-0'),
        pytest.param(1, id='int-1'),
        pytest.param('true', id='str-true'),
        pytest.param('false', id='str-false'),
        pytest.param(1.0, id='float'),
    ],
)
def test_config_bool_fields_reject_coercible_values(
    field_name: str,
    bad_value: object,
) -> None:
    # Programmatic ingress rejects 0/1/strings; from_mapping env-parses strings.
    construct_config = cast('Callable[..., Config]', Config)
    with pytest.raises(TypeError, match=field_name):
        construct_config(**{field_name: bad_value})
    if not isinstance(bad_value, str):
        with pytest.raises(TypeError, match=field_name):
            Config.from_mapping({field_name: bad_value})


@pytest.mark.parametrize('field_name', _BOOL_FIELDS)
def test_config_bool_fields_accept_exact_bool(field_name: str) -> None:
    if field_name == 'reuse_port' and sys.platform == 'win32':
        pytest.skip('reuse_port is unavailable on Windows')
    construct_config = cast('Callable[..., Config]', Config)
    for value in (True, False):
        config = construct_config(**{field_name: value})
        assert getattr(config, field_name) is value
        mapped = Config.from_mapping({field_name: value})
        assert getattr(mapped, field_name) is value


@pytest.mark.parametrize('field_name', ['user', 'group'])
@pytest.mark.parametrize('bad_value', [True, False])
def test_config_principal_rejects_bool(field_name: str, bad_value: bool) -> None:
    construct_config = cast('Callable[..., Config]', Config)
    with pytest.raises(TypeError, match=field_name):
        construct_config(**{field_name: bad_value})
    with pytest.raises(TypeError, match=field_name):
        Config.from_mapping({field_name: bad_value})


def test_config_convenience_port_requires_exact_int_in_range() -> None:
    assert Config(port=0).bind == ('127.0.0.1:0',)
    assert Config(port=65_535).bind == ('127.0.0.1:65535',)
    with pytest.raises(TypeError, match='port'):
        Config(port=True)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match='port'):
        Config(port=1.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match='port'):
        Config(port=_IntSubclass(8000))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match='port'):
        Config(port=-1)
    with pytest.raises(ValueError, match='port'):
        Config(port=65_536)
    with pytest.raises(TypeError, match='port'):
        Config.from_mapping({'port': 1.0})
    with pytest.raises(ValueError, match='port'):
        Config.from_mapping({'port': 65_536})


def test_config_convenience_inputs_normalize_once_into_bind() -> None:
    config = Config(host='[::1]', port=0)

    assert config.bind == ('[::1]:0',)
    assert dataclasses.replace(config, workers=2).bind == config.bind
    assert dataclasses.replace(config, bind=('127.0.0.1:9010',)).bind == (
        '127.0.0.1:9010',
    )
    with pytest.raises(ValueError, match='bind cannot be combined'):
        Config(bind=('127.0.0.1:9010',), port=9020)


def test_config_from_toml_reads_flat_top_level_keys(tmp_path: Path) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
port = 9010
workers = 2
max_requests = 11
max_requests_jitter = 3
timeout_worker_healthcheck = 4.5
http1 = false
lifespan = "on"
pid = "server.pid"
certfile = "server.crt"
keyfile = "server.key"
ca_certs = "clients.pem"
cert_reqs = "optional"
user = "www-data"
group = "www-data"
umask = "027"
proxy_headers = true
forwarded_allow_ips = ["127.0.0.1"]
forwarded_fields = ["for", "proto", "host"]
timeout_keep_alive = 1.5
timeout_request_header = 2.5
timeout_request_body_idle = 3.5
h2_timeout_response_stall = 4.5
limit_concurrency = 9
limit_connections = 11
runtime_threads = 4
timeout_lifespan_startup = 6.5
timeout_lifespan_shutdown = 7.5
websocket_per_message_deflate = false
websocket_ping_interval = 8.5
websocket_ping_timeout = 9.5
server_header = "full"
date_header = false
response_headers = ["x-demo: one", "x-extra: two"]
""".strip()
    )

    config = Config.from_toml(config_path)

    assert config.bind == ('127.0.0.1:9010',)
    assert config.workers == 2
    assert config.max_requests == 11
    assert config.max_requests_jitter == 3
    assert config.timeout_worker_healthcheck == 4.5
    assert config.http1 is False
    assert config.pid == Path('server.pid')
    assert config.certfile == Path('server.crt')
    assert config.keyfile == Path('server.key')
    assert config.ca_certs == Path('clients.pem')
    assert config.cert_reqs == 'optional'
    assert config.user == 'www-data'
    assert config.group == 'www-data'
    assert config.umask == 0o27
    assert config.proxy_headers is True
    assert config.forwarded_fields == ('for', 'proto', 'host')
    assert config.timeout_keep_alive == 1.5
    assert config.timeout_request_header == 2.5
    assert config.timeout_request_body_idle == 3.5
    assert config.h2_timeout_response_stall == 4.5
    assert config.limit_concurrency == 9
    assert config.limit_connections == 11
    assert config.runtime_threads == 4
    assert config.lifespan == 'on'
    assert config.timeout_lifespan_startup == 6.5
    assert config.timeout_lifespan_shutdown == 7.5
    assert config.websocket_per_message_deflate is False
    assert config.websocket_ping_interval == 8.5
    assert config.websocket_ping_timeout == 9.5
    assert config.server_header == 'full'
    assert config.date_header is False
    assert config.response_headers == ('x-demo: one', 'x-extra: two')


def test_config_from_toml_rejects_websocket_message_size_inherit(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('websocket_max_message_size = "inherit"')

    with pytest.raises(ValueError, match='invalid literal for int'):
        Config.from_toml(config_path)


def test_config_from_env_reads_layered_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('H2CORN_PORT', '9000')
    monkeypatch.setenv('H2CORN_WORKERS', '3')
    monkeypatch.setenv('H2CORN_MAX_REQUESTS', '8')
    monkeypatch.setenv('H2CORN_MAX_REQUESTS_JITTER', '2')
    monkeypatch.setenv('H2CORN_TIMEOUT_WORKER_HEALTHCHECK', '3.5')
    monkeypatch.setenv('H2CORN_HTTP1', 'false')
    monkeypatch.setenv('H2CORN_PID', 'server.pid')
    monkeypatch.setenv('H2CORN_CERTFILE', 'server.crt')
    monkeypatch.setenv('H2CORN_KEYFILE', 'server.key')
    monkeypatch.setenv('H2CORN_CA_CERTS', 'clients.pem')
    monkeypatch.setenv('H2CORN_CERT_REQS', 'required')
    monkeypatch.setenv('H2CORN_USER', 'www-data')
    monkeypatch.setenv('H2CORN_GROUP', 'www-data')
    monkeypatch.setenv('H2CORN_UMASK', '027')
    monkeypatch.setenv('H2CORN_ACCESS_LOG', 'false')
    monkeypatch.setenv('H2CORN_PROXY_HEADERS', 'true')
    monkeypatch.setenv('H2CORN_FORWARDED_ALLOW_IPS', '127.0.0.1,unix')
    monkeypatch.setenv('H2CORN_FORWARDED_FIELDS', 'FOR,PROTO,HOST')
    monkeypatch.setenv('H2CORN_PROXY_PROTOCOL', 'v1')
    monkeypatch.setenv('H2CORN_TIMEOUT_HANDSHAKE', '3.5')
    monkeypatch.setenv('H2CORN_TIMEOUT_KEEP_ALIVE', '1.5')
    monkeypatch.setenv('H2CORN_TIMEOUT_REQUEST_HEADER', '2.5')
    monkeypatch.setenv('H2CORN_TIMEOUT_REQUEST_BODY_IDLE', '3.5')
    monkeypatch.setenv('H2CORN_LIMIT_CONCURRENCY', '7')
    monkeypatch.setenv('H2CORN_LIMIT_CONNECTIONS', '9')
    monkeypatch.setenv('H2CORN_RUNTIME_THREADS', '5')
    monkeypatch.setenv('H2CORN_LIFESPAN', 'off')
    monkeypatch.setenv('H2CORN_TIMEOUT_LIFESPAN_STARTUP', '5.5')
    monkeypatch.setenv('H2CORN_TIMEOUT_LIFESPAN_SHUTDOWN', '6.5')
    monkeypatch.setenv('H2CORN_LIMIT_REQUEST_LINE', '4094')
    monkeypatch.setenv('H2CORN_H2_MAX_HEADER_LIST_SIZE', '65536')
    monkeypatch.setenv('H2CORN_MAX_REQUEST_BODY_SIZE', '1048576')
    monkeypatch.setenv('H2CORN_WEBSOCKET_MAX_MESSAGE_SIZE', '2048')
    monkeypatch.setenv('H2CORN_WEBSOCKET_PER_MESSAGE_DEFLATE', 'false')
    monkeypatch.setenv('H2CORN_WEBSOCKET_PING_INTERVAL', '8.5')
    monkeypatch.setenv('H2CORN_WEBSOCKET_PING_TIMEOUT', '9.5')
    monkeypatch.setenv('H2CORN_SERVER_HEADER', 'full')
    monkeypatch.setenv('H2CORN_DATE_HEADER', 'false')
    # One header per line: a comma belongs to a header value.
    monkeypatch.setenv('H2CORN_RESPONSE_HEADERS', 'x-demo: one\nx-extra: two')

    config = Config.from_env(os.environ)

    assert config.bind == ('127.0.0.1:9000',)
    assert config.workers == 3
    assert config.max_requests == 8
    assert config.max_requests_jitter == 2
    assert config.timeout_worker_healthcheck == 3.5
    assert config.http1 is False
    assert config.pid == Path('server.pid')
    assert config.certfile == Path('server.crt')
    assert config.keyfile == Path('server.key')
    assert config.ca_certs == Path('clients.pem')
    assert config.cert_reqs == 'required'
    assert config.user == 'www-data'
    assert config.group == 'www-data'
    assert config.umask == 0o27
    assert config.access_log is False
    assert config.proxy_headers is True
    assert config.forwarded_allow_ips == ('127.0.0.1', 'unix')
    assert config.forwarded_fields == ('for', 'proto', 'host')
    assert config.proxy_protocol == 'v1'
    assert config.timeout_handshake == 3.5
    assert config.timeout_keep_alive == 1.5
    assert config.timeout_request_header == 2.5
    assert config.timeout_request_body_idle == 3.5
    assert config.limit_concurrency == 7
    assert config.limit_connections == 9
    assert config.runtime_threads == 5
    assert config.lifespan == 'off'
    assert config.timeout_lifespan_startup == 5.5
    assert config.timeout_lifespan_shutdown == 6.5
    assert config.limit_request_line == 4094
    assert config.h2_max_header_list_size == 65536
    assert config.max_request_body_size == 1048576
    assert config.websocket_max_message_size == 2048
    assert config.websocket_per_message_deflate is False
    assert config.websocket_ping_interval == 8.5
    assert config.websocket_ping_timeout == 9.5
    assert config.server_header == 'full'
    assert config.date_header is False
    assert config.response_headers == ('x-demo: one', 'x-extra: two')


def test_config_from_env_applies_explicit_empty_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('H2CORN_FORWARDED_ALLOW_IPS', '')

    config = Config.from_env(os.environ)

    assert config.forwarded_allow_ips == ()


def test_config_from_env_accepts_csv_bind_values() -> None:
    config = Config.from_env({'H2CORN_BIND': '127.0.0.1:9000,[::1]:9000'})

    assert config.bind == ('127.0.0.1:9000', '[::1]:9000')


def test_config_from_env_rejects_websocket_message_size_inherit() -> None:
    with pytest.raises(ValueError, match='invalid literal for int'):
        Config.from_env({'H2CORN_WEBSOCKET_MAX_MESSAGE_SIZE': 'inherit'})


@pytest.mark.parametrize(
    'seconds',
    [
        pytest.param(1e20, id='too-large-for-a-duration'),
        pytest.param(1e300, id='astronomically-large'),
        pytest.param(float('inf'), id='infinite'),
        pytest.param(float('nan'), id='not-a-number'),
    ],
)
def test_native_config_rejects_unrepresentable_durations(seconds: float) -> None:
    """A duration the native layer cannot represent is a config error, not a panic."""
    from h2corn._lib import prepare_tls

    with pytest.raises(ValueError, match='timeout_keep_alive'):
        prepare_tls(Config(timeout_keep_alive=seconds))


def test_config_rejects_empty_unix_bind_path() -> None:
    with pytest.raises(ValueError, match='invalid unix bind target'):
        Config(bind=('unix:',))


def test_config_rejects_partial_tls_keypair() -> None:
    with pytest.raises(ValueError, match='certfile and keyfile'):
        Config(certfile='server.crt')


def test_config_rejects_tls_on_unix_listener() -> None:
    with pytest.raises(ValueError, match='TLS is supported only on TCP'):
        Config(
            bind=('unix:/tmp/h2corn.sock',),
            certfile='server.crt',
            keyfile='server.key',
        )


def test_config_rejects_mtls_without_ca_bundle() -> None:
    with pytest.raises(ValueError, match='requires ca_certs'):
        Config(cert_reqs='required')


def test_config_rejects_mtls_without_tls_keypair() -> None:
    with pytest.raises(ValueError, match='requires certfile and keyfile'):
        Config(ca_certs='clients.pem', cert_reqs='required')


def test_config_rejects_unused_ca_bundle() -> None:
    with pytest.raises(ValueError, match='ca_certs requires cert_reqs'):
        Config(ca_certs='clients.pem')


def test_parse_cli_accepts_tls_options() -> None:
    cli_settings, import_settings, config = parse_cli(
        [
            '--certfile',
            'server.crt',
            '--keyfile',
            'server.key',
            '--ca-certs',
            'clients.pem',
            '--cert-reqs',
            'optional',
            'example:app',
        ],
        {},
    )

    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(target='example:app')
    assert config.certfile == Path('server.crt')
    assert config.keyfile == Path('server.key')
    assert config.ca_certs == Path('clients.pem')
    assert config.cert_reqs == 'optional'


def test_parse_cli_accepts_forwarded_fields() -> None:
    _cli_settings, _import_settings, config = parse_cli(
        ['--proxy-headers', '--forwarded-fields', 'host,port', 'example:app'],
        {},
    )

    assert config.forwarded_fields == ('host', 'port')


def test_config_rejects_invalid_trusted_proxy_entry() -> None:
    with pytest.raises(ValueError, match='forwarded_allow_ips'):
        Config(forwarded_allow_ips=('example.invalid',))


def test_config_allows_empty_trusted_proxy_set() -> None:
    config = Config(forwarded_allow_ips=())

    assert config.forwarded_allow_ips == ()


def test_config_normalizes_forwarded_fields_and_allows_empty() -> None:
    assert Config(forwarded_fields='FOR, proto, for').forwarded_fields == (
        'for',
        'proto',
    )
    assert Config.from_mapping({'forwarded_fields': 'HOST,PORT'}).forwarded_fields == (
        'host',
        'port',
    )
    assert Config(forwarded_fields=()).forwarded_fields == ()


def test_config_rejects_unknown_forwarded_field_with_accepted_values() -> None:
    with pytest.raises(
        ValueError,
        match=r'invalid forwarded_fields entry.*for.*proto.*host.*port.*prefix.*forwarded',
    ):
        Config(forwarded_fields=('for', 'authority'))


def test_config_rejects_mixed_forwarding_dialects() -> None:
    with pytest.raises(ValueError, match=r"forwarded.*'host'"):
        Config(forwarded_fields=('forwarded', 'host'))


def test_config_rejects_proxy_headers_that_can_believe_nothing() -> None:
    with pytest.raises(ValueError, match=r'proxy_headers requires.*forwarded_fields'):
        Config(proxy_headers=True, forwarded_fields=())


def test_config_rejects_proxy_headers_that_can_trust_no_peer() -> None:
    with pytest.raises(
        ValueError, match=r'proxy_headers requires.*forwarded_allow_ips'
    ):
        Config(proxy_headers=True, forwarded_allow_ips=())


def test_forwarding_options_stay_inert_while_proxy_headers_is_off() -> None:
    """
    Both forwarding options default to a non-empty value, so a symmetric check
    on the other direction would reject the default configuration itself. A
    boundary described but not in force is also how one file serves several
    environments.
    """
    assert Config(proxy_headers=False, forwarded_fields=()).forwarded_fields == ()
    assert Config(proxy_headers=False, forwarded_allow_ips=()).forwarded_allow_ips == ()
    assert Config(forwarded_fields=('host',)).forwarded_fields == ('host',)


def test_config_normalizes_multiple_bind_entries() -> None:
    config = Config(bind=['127.0.0.1:8000', '[::1]:8000', 'unix:/tmp/h2corn.sock'])

    assert config.bind == (
        '127.0.0.1:8000',
        '[::1]:8000',
        'unix:/tmp/h2corn.sock',
    )
    assert tcp_bind_convenience(config.bind) is None


def test_config_allows_ping_timeout_with_disabled_interval() -> None:
    # Interval zero is the typed off state; timeout may still be set and is
    # ignored at runtime (keep_alive is entirely off).
    config = Config(websocket_ping_interval=0.0, websocket_ping_timeout=1.0)
    assert config.websocket_ping_interval == 0.0
    assert config.websocket_ping_timeout == 1.0
    # Construction and preparation succeed; no cross-field rejection.
    from h2corn._lib import prepare_tls

    prepare_tls(config)


def test_config_normalizes_numeric_user_and_group_strings() -> None:
    config = Config(user='1000', group='1001')

    assert config.user == 1000
    assert config.group == 1001


def test_config_rejects_unknown_mapping_keys() -> None:
    with pytest.raises(ValueError, match='unknown config keys'):
        Config.from_mapping({'proxy': {'proxy_headers': True}})


def test_config_option_schema_has_unique_external_names() -> None:
    options = config_options()

    assert len({option.name for option in options}) == len(options)
    assert len({option.env_var for option in options}) == len(options)
    assert len({flag for option in options for flag in option.cli_flags}) == sum(
        len(option.cli_flags) for option in options
    )


def test_config_defaults_follow_config_option_schema() -> None:
    config = Config()

    for option in config_options():
        assert getattr(config, option.name) == option.default


def test_websocket_max_message_size_defaults_to_safe_cap() -> None:
    config = Config()

    assert config.websocket_max_message_size == 16_777_216


def test_config_does_not_trust_forwarding_headers_by_default() -> None:
    config = Config()

    assert config.proxy_headers is False


def test_config_defaults_prioritize_upload_throughput() -> None:
    config = Config()

    assert config.h2_initial_stream_window_size == 8 * 1024 * 1024
    assert config.h2_initial_connection_window_size == 8 * 1024 * 1024


def test_config_rejects_stream_window_larger_than_connection_window() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "h2_initial_stream_window_size '8388608' exceeds "
            "h2_initial_connection_window_size '1048576': "
            'a stream window cannot exceed its connection window'
        ),
    ):
        Config(
            h2_initial_stream_window_size=8 * 1024 * 1024,
            h2_initial_connection_window_size=1024 * 1024,
        )


def test_config_rejects_websocket_message_size_inherit() -> None:
    with pytest.raises(ValueError, match='invalid literal for int'):
        Config.from_mapping({'websocket_max_message_size': 'inherit'})


def test_cli_parser_defaults_and_flags_follow_config_option_schema() -> None:
    base = Config(port=9011, http1=False, access_log=False)
    parser = build_parser(base, None)
    option_actions = {
        option.name: next(
            action
            for action in parser._actions
            if set(option.cli_flags).issubset(action.option_strings)
            or any(flag in action.option_strings for flag in option.cli_flags)
        )
        for option in config_options()
    }

    for option in config_options():
        action = option_actions[option.name]
        assert action.default == getattr(base, option.name)
        assert action.help == option.help_text()
        if option.metadata.cli_action == 'bool':
            assert isinstance(action, argparse.BooleanOptionalAction)
        else:
            assert action.type == option.metadata.cli_type
            assert action.choices == option.metadata.cli_choices
            assert action.metavar == option.metadata.cli_metavar


def test_parse_cli_applies_env_listener_convenience_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('port = 9010')

    cli_settings, import_settings, config = parse_cli(
        ['--config', str(config_path), 'example:app'],
        {'H2CORN_PORT': '9020'},
    )

    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(target='example:app')
    assert config.bind == ('127.0.0.1:9020',)


def test_parse_cli_rejects_websocket_message_size_inherit() -> None:
    with pytest.raises(SystemExit) as raised:
        parse_cli(['--websocket-max-message-size', 'inherit', 'example:app'], {})

    assert raised.value.code == 2


def test_parse_cli_reports_configuration_file_errors_through_the_full_parser(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / 'broken.toml'
    config_path.write_text('workers = 0')

    with pytest.raises(SystemExit) as raised:
        parse_cli(['--config', str(config_path), 'example:app'], {})

    assert raised.value.code == 2
    stderr = capsys.readouterr().err
    assert stderr.startswith('usage: h2corn ')
    assert str(config_path) in stderr
    assert 'workers' in stderr


def test_parse_cli_reports_cli_configuration_errors_without_a_traceback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        parse_cli(['--workers', '0', 'example:app'], {})

    assert raised.value.code == 2
    stderr = capsys.readouterr().err
    assert stderr.startswith('usage: h2corn ')
    assert 'workers' in stderr


def test_parse_cli_accepts_factory_flag() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--factory', 'example:create_app'],
        {},
    )

    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(target='example:create_app', factory=True)
    assert isinstance(config, Config)


def test_parse_cli_accepts_app_dir() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--app-dir', 'src', 'example:app'],
        {},
    )

    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(
        target='example:app',
        app_dir=Path('src').resolve(),
    )
    assert isinstance(config, Config)


def test_parse_cli_accepts_env_file() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--env-file', '.env', 'example:app'],
        {},
    )

    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(
        target='example:app',
        env_file=Path('.env').resolve(),
    )
    assert isinstance(config, Config)


def test_parse_cli_accepts_check_config_flag() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--check-config', 'example:app'],
        {},
    )

    assert cli_settings == CliSettings(check_config=True)
    assert import_settings == ImportSettings(target='example:app')
    assert isinstance(config, Config)


def test_print_config_round_trips_every_control_character() -> None:
    """`--print-config` output must parse back as TOML.

    TOML forbids raw control characters and gives only some of them a
    shorthand escape, so anything else needs `\\uXXXX`.
    """
    from h2corn._cli import _toml_literal

    for code in (*range(0x20), 0x7F, 0x2028, 0x1F600):
        value = f'a{chr(code)}b'
        parsed = tomllib.loads(f'key = {_toml_literal(value)}')
        assert parsed['key'] == value, f'U+{code:04X} did not round-trip'


def test_parse_cli_accepts_print_config_flag() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--print-config', 'example:app'],
        {},
    )

    assert cli_settings == CliSettings(print_config=True)
    assert import_settings == ImportSettings(target='example:app')
    assert isinstance(config, Config)


def test_parse_cli_accepts_check_config_without_target() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--check-config'],
        {},
    )

    assert cli_settings == CliSettings(check_config=True)
    assert import_settings == ImportSettings(target='')
    assert isinstance(config, Config)


def test_parse_cli_builds_one_parser_after_configuration_resolves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal startup pays for argparse once; error presentation stays lazy."""
    from h2corn import _cli

    real_build_parser = _cli.build_parser
    bases: list[Config] = []

    def build_once(base: Config, config_path: Path | None):
        bases.append(base)
        return real_build_parser(base, config_path)

    monkeypatch.setattr(_cli, 'build_parser', build_once)

    _, _, config = _cli.parse_cli(['--check-config'], {})

    assert bases == [config]


def test_parse_cli_version_exits_without_target(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        parse_cli(['--version'], {})

    assert exc.value.code == 0
    assert capsys.readouterr().out.startswith('h2corn ')


def test_parse_cli_accepts_print_config_without_target() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--print-config'],
        {},
    )

    assert cli_settings == CliSettings(print_config=True)
    assert import_settings == ImportSettings(target='')
    assert isinstance(config, Config)


def test_parse_cli_accepts_reload_flag() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--reload', 'example:app'],
        {},
    )

    assert cli_settings == CliSettings(reload=True)
    assert import_settings == ImportSettings(target='example:app')
    assert isinstance(config, Config)


def test_parse_cli_accepts_reload_dirs() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--reload', '--reload-dir', 'src', '--reload-dir', 'locale', 'example:app'],
        {},
    )

    assert cli_settings == CliSettings(
        reload=True,
        reload_dirs=(Path('src').resolve(), Path('locale').resolve()),
    )
    assert import_settings == ImportSettings(target='example:app')
    assert isinstance(config, Config)


def test_parse_cli_accepts_pid_path() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--pid', 'server.pid', 'example:app'],
        {},
    )

    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(target='example:app')
    assert config.pid == Path('server.pid')


def test_parse_cli_accepts_user_group_and_umask() -> None:
    cli_settings, import_settings, config = parse_cli(
        [
            '--user',
            'www-data',
            '--group',
            'www-data',
            '--umask',
            '027',
            'example:app',
        ],
        {},
    )

    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(target='example:app')
    assert config.user == 'www-data'
    assert config.group == 'www-data'
    assert config.umask == 0o27


def test_parse_cli_rejects_reload_with_multiple_workers() -> None:
    with pytest.raises(SystemExit):
        parse_cli(['--reload', '--workers', '2', 'example:app'], {})


def test_parse_cli_rejects_reload_with_check_config() -> None:
    with pytest.raises(SystemExit):
        parse_cli(['--reload', '--check-config', 'example:app'], {})


def test_parse_cli_accepts_reload_patterns() -> None:
    cli_settings, import_settings, config = parse_cli(
        [
            '--reload',
            '--reload-include',
            '*.mo',
            '--reload-exclude',
            'tests',
            '--reload-exclude',
            'scripts',
            'example:app',
        ],
        {},
    )

    assert cli_settings == CliSettings(
        reload=True,
        reload_includes=('*.py', '*.mo'),
        reload_excludes=(
            '.*',
            '.py[cod]',
            '.sw.*',
            '~*',
            'tests',
            'scripts',
        ),
    )
    assert import_settings == ImportSettings(target='example:app')
    assert isinstance(config, Config)


def test_parse_cli_rejects_reload_patterns_without_reload() -> None:
    with pytest.raises(SystemExit):
        parse_cli(['--reload-exclude', 'tests', 'example:app'], {})


def test_parse_cli_rejects_reload_dirs_without_reload() -> None:
    with pytest.raises(SystemExit):
        parse_cli(['--reload-dir', 'src', 'example:app'], {})


def test_parse_cli_rejects_host_port_override_for_multi_bind_base(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('bind = ["127.0.0.1:9010", "127.0.0.1:9011"]')

    with pytest.raises(SystemExit):
        parse_cli(
            ['--config', str(config_path), '--port', '9020', 'example:app'],
            {},
        )


def test_parse_cli_rejects_env_listener_convenience_override_for_multi_bind_base(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('bind = ["127.0.0.1:9010", "127.0.0.1:9011"]')

    with pytest.raises(SystemExit):
        parse_cli(
            ['--config', str(config_path), 'example:app'],
            {'H2CORN_PORT': '9020'},
        )


def test_repeating_a_port_zero_bind_is_rejected() -> None:
    """
    Listeners that ask for port 0 share the one port the kernel assigns, so a
    repeated `host:0` is the same listener twice and the second bind fails
    with EADDRINUSE. Distinct hosts on port 0 remain the supported case.
    """
    with pytest.raises(ValueError, match='duplicate bind entry'):
        Config(bind=('127.0.0.1:0', '127.0.0.1:0'))

    assert Config(bind=('127.0.0.1:0', '[::1]:0')).bind == ('127.0.0.1:0', '[::1]:0')


def test_response_header_values_may_contain_commas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    A comma is ordinary inside a header value, so response headers are one
    per line rather than comma-separated: splitting on commas turned
    `cache-control: public, max-age=60` into two entries, the second of which
    was not a header at all and failed validation.
    """
    monkeypatch.setenv(
        'H2CORN_RESPONSE_HEADERS', 'cache-control: public, max-age=60\nx-demo: 1'
    )
    from_env = Config.from_env(os.environ)
    assert from_env.response_headers == (
        'cache-control: public, max-age=60',
        'x-demo: 1',
    )

    # A lone string is one header, matching how the CLI's repeated --header
    # flag and an explicit tuple both behave.
    assert Config(response_headers='cache-control: public, max-age=60').response_headers
    assert Config(
        response_headers='cache-control: public, max-age=60'
    ).response_headers == ('cache-control: public, max-age=60',)
