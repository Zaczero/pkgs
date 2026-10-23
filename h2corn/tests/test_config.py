import argparse
import base64
import dataclasses
import os
import pickle
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from h2corn import Config, RequestPolicy
from h2corn._cli import CliSettings, ImportSettings, build_parser, parse_cli
from h2corn._config import config_options, tcp_bind_convenience

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class _NamedConfig(Config):
    label: str = 'custom'


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class _NamedPolicy(RequestPolicy):
    label: str = 'custom'


@pytest.mark.parametrize(
    'value',
    [
        Config(workers=3, backlog=1234),
        _NamedConfig(workers=3, label='service'),
        RequestPolicy(
            path='/service/*',
            max_request_body_size=123,
            timeout_request_body_idle=7,
            h2_timeout_response_stall=9,
        ),
        _NamedPolicy(
            path='/service/*',
            max_request_body_size=123,
            timeout_request_body_idle=7,
            h2_timeout_response_stall=9,
            label='stream',
        ),
    ],
)
def test_config_pickle_preserves_named_options(value) -> None:
    restored = pickle.loads(pickle.dumps(value))  # ruff: ignore[suspicious-pickle-usage] - Locally generated configuration.
    assert type(restored) is type(value)
    assert restored == value


def test_config_pickle_named_state_is_independent_of_field_order() -> None:
    from h2corn._config import _restore_config_v1, _restore_request_policy_v1

    restored = _restore_config_v1({'backlog': 1234, 'workers': 3})
    assert restored.backlog == 1234
    assert restored.workers == 3
    assert restored.bind == ('127.0.0.1:8000',)
    policy = _restore_request_policy_v1({
        'h2_timeout_response_stall': 9,
        'path': '/service/*',
        'timeout_request_body_idle': 7,
        'max_request_body_size': 123,
    })
    assert policy.path == '/service/*'
    assert policy.max_request_body_size == 123
    assert policy.timeout_request_body_idle == 7
    assert policy.h2_timeout_response_stall == 9


def test_v1_config_pickle_preserves_released_option_names() -> None:
    payload = (
        'gASVrAUAAAAAAACMDmgyY29ybi5fY29uZmlnlIwSX3Jlc3RvcmVfY29uZmlnX3YxlJOUfZQojAlyb290X3BhdGiU'
        'jACUjAhsaWZlc3BhbpSMBGF1dG+UjBh0aW1lb3V0X2xpZmVzcGFuX3N0YXJ0dXCUR0BOAAAAAAAAjBl0aW1lb3V0'
        'X2xpZmVzcGFuX3NodXRkb3dulEdAPgAAAAAAAIwEYmluZJSMDjEyNy4wLjAuMTo4MDAwlIWUjA91ZHNfcGVybWlz'
        'c2lvbnOUTowHYmFja2xvZ5RN0gSMCnJldXNlX3BvcnSUiYwIY2VydGZpbGWUTowHa2V5ZmlsZZROjAhjYV9jZXJ0'
        'c5ROjAljZXJ0X3JlcXOUjARub25llIwDcGlklE6MBHVzZXKUTowFZ3JvdXCUTowFdW1hc2uUTowHd29ya2Vyc5RL'
        'A4wPcnVudGltZV90aHJlYWRzlEsCjARsb29wlGgHjAxsb29wX3RocmVhZHOUSwGMDG1heF9yZXF1ZXN0c5RLAIwT'
        'bWF4X3JlcXVlc3RzX2ppdHRlcpRLAIwadGltZW91dF93b3JrZXJfaGVhbHRoY2hlY2uUR0A+AAAAAAAAjAVodHRw'
        'MZSIjARncnBjlImMCmFjY2Vzc19sb2eUiIwKbG9nX2Zvcm1hdJSMBHRleHSUjBZtYXhfY29uY3VycmVudF9zdHJl'
        'YW1zlE0AAYwXbGltaXRfcmVxdWVzdF9oZWFkX3NpemWUSgAAEACMEmxpbWl0X3JlcXVlc3RfbGluZZRNAECMFGxp'
        'bWl0X3JlcXVlc3RfZmllbGRzlEtkjBhsaW1pdF9yZXF1ZXN0X2ZpZWxkX3NpemWUTQCAjBdoMl9tYXhfaGVhZGVy'
        'X2xpc3Rfc2l6ZZRKAAAQAIwYaDJfbWF4X2hlYWRlcl9ibG9ja19zaXpllEoAABAAjBloMl9tYXhfaW5ib3VuZF9m'
        'cmFtZV9zaXpllEoAAAEAjB1oMl9pbml0aWFsX3N0cmVhbV93aW5kb3dfc2l6ZZRKAACAAIwhaDJfaW5pdGlhbF9j'
        'b25uZWN0aW9uX3dpbmRvd19zaXpllEoAAIAAjBVtYXhfcmVxdWVzdF9ib2R5X3NpemWUSgAAAECMEHJlcXVlc3Rf'
        'cG9saWNpZXOUKYwRbGltaXRfY29uY3VycmVuY3mUSwCMEWxpbWl0X2Nvbm5lY3Rpb25zlEsAjBF0aW1lb3V0X2hh'
        'bmRzaGFrZZRHQBQAAAAAAACMGXRpbWVvdXRfZ3JhY2VmdWxfc2h1dGRvd26UR0A+AAAAAAAAjBJ0aW1lb3V0X2tl'
        'ZXBfYWxpdmWUR0BeAAAAAAAAjBZ0aW1lb3V0X3JlcXVlc3RfaGVhZGVylEdAJAAAAAAAAIwZdGltZW91dF9yZXF1'
        'ZXN0X2JvZHlfaWRsZZRHQE4AAAAAAACMGWgyX3RpbWVvdXRfcmVzcG9uc2Vfc3RhbGyUR0BOAAAAAAAAjBBoMl90'
        'aW1lb3V0X3dyaXRllEdATgAAAAAAAIwad2Vic29ja2V0X21heF9tZXNzYWdlX3NpemWUSgAAAAGMHXdlYnNvY2tl'
        'dF9wZXJfbWVzc2FnZV9kZWZsYXRllIiMF3dlYnNvY2tldF9waW5nX2ludGVydmFslEdATgAAAAAAAIwWd2Vic29j'
        'a2V0X3BpbmdfdGltZW91dJRHQD4AAAAAAACMDXByb3h5X2hlYWRlcnOUiYwTZm9yd2FyZGVkX2FsbG93X2lwc5SM'
        'CTEyNy4wLjAuMZSMAzo6MZSMBHVuaXiUh5SMEGZvcndhcmRlZF9maWVsZHOUjANmb3KUjAVwcm90b5SGlIwOcHJv'
        'eHlfcHJvdG9jb2yUjANvZmaUjA1zZXJ2ZXJfaGVhZGVylGhJjAtkYXRlX2hlYWRlcpSIjBByZXNwb25zZV9oZWFk'
        'ZXJzlCl1hZRSlC4='
    )
    restored = pickle.loads(base64.b64decode(payload))  # ruff: ignore[suspicious-pickle-usage] - Frozen trusted fixture.
    assert restored.workers == 3
    assert restored.backlog == 1234
    assert restored.bind == ('127.0.0.1:8000',)


def test_v1_request_policy_pickle_preserves_field_meanings() -> None:
    payload = (
        'gASVsQAAAAAAAACMDmgyY29ybi5fY29uZmlnlIwaX3Jlc3RvcmVfcmVxdWVzdF9wb2xpY3lfdjGUk5R9lCiMBHBh'
        'dGiUjAovc2VydmljZS8qlIwVbWF4X3JlcXVlc3RfYm9keV9zaXpllEt7jBl0aW1lb3V0X3JlcXVlc3RfYm9keV9p'
        'ZGxllEdAHAAAAAAAAIwZaDJfdGltZW91dF9yZXNwb25zZV9zdGFsbJRHQCIAAAAAAAB1hZRSlC4='
    )
    restored = pickle.loads(base64.b64decode(payload))  # ruff: ignore[suspicious-pickle-usage] - Frozen trusted fixture.
    assert restored == RequestPolicy(
        path='/service/*',
        max_request_body_size=123,
        timeout_request_body_idle=7,
        h2_timeout_response_stall=9,
    )


def test_unknown_server_environment_variable_is_actionable() -> None:
    with pytest.raises(ValueError, match='H2CORN_MAX_REQUEST_BODY_SIZ'):
        Config.from_env({'H2CORN_MAX_REQUEST_BODY_SIZ': '17'})
    assert Config.from_env({'OTHER_APPLICATION_OPTION': '17'}) == Config()


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
        ({'h2_timeout_write': -1}, 'h2_timeout_write'),
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
        ({'max_request_body_size': 1 << 64}, 'max_request_body_size'),
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
    'h2_timeout_write',
    'websocket_ping_interval',
    'websocket_ping_timeout',
)

_BOOL_FIELDS: tuple[str, ...] = (
    'reuse_port',
    'http1',
    'grpc',
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


def test_config_from_toml_reads_options_and_request_policy_tables(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
port = 9010
workers = 2
max_requests = 11
max_requests_jitter = 3
timeout_worker_healthcheck = 4.5
http1 = false
grpc = true
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
h2_timeout_write = 7.5
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

[[request_policies]]
path = "/example.Chat/*"
max_request_body_size = 0
timeout_request_body_idle = 0
h2_timeout_response_stall = 4.25
""".strip()
    )

    config = Config.from_toml(config_path)

    assert config.bind == ('127.0.0.1:9010',)
    assert config.workers == 2
    assert config.max_requests == 11
    assert config.max_requests_jitter == 3
    assert config.timeout_worker_healthcheck == 4.5
    assert config.http1 is False
    assert config.grpc is True
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
    assert config.h2_timeout_write == 7.5
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
    assert config.request_policies == (
        RequestPolicy(
            path='/example.Chat/*',
            max_request_body_size=0,
            timeout_request_body_idle=0,
            h2_timeout_response_stall=4.25,
        ),
    )


def test_config_from_toml_rejects_websocket_message_size_inherit(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('websocket_max_message_size = "inherit"')

    with pytest.raises(ValueError, match='invalid literal for int'):
        Config.from_toml(config_path)


def test_config_from_mapping_converts_request_policy_tables() -> None:
    config = Config.from_mapping({
        'request_policies': [
            {
                'path': '/upload/*',
                'max_request_body_size': 0,
                'timeout_request_body_idle': 0,
                'h2_timeout_response_stall': 2.5,
            }
        ]
    })

    assert config.request_policies == (
        RequestPolicy(
            path='/upload/*',
            max_request_body_size=0,
            timeout_request_body_idle=0,
            h2_timeout_response_stall=2.5,
        ),
    )


@pytest.mark.parametrize(
    'path',
    [
        pytest.param('upload', id='relative'),
        pytest.param('/upload?format=json', id='query'),
        pytest.param('/upload#fragment', id='fragment'),
        pytest.param('/upload*part', id='embedded-wildcard'),
        pytest.param('/upload/**', id='multiple-wildcards'),
        pytest.param('/upload/%2', id='invalid-percent-escape'),
        pytest.param('/upload/é', id='non-ascii'),
    ],
)
def test_request_policy_rejects_invalid_paths(path: str) -> None:
    with pytest.raises((TypeError, ValueError), match='request policy path'):
        RequestPolicy(
            path=path,
            max_request_body_size=0,
            timeout_request_body_idle=0,
            h2_timeout_response_stall=0,
        )


@pytest.mark.parametrize(
    ('field', 'value', 'error'),
    [
        ('max_request_body_size', True, TypeError),
        ('max_request_body_size', -1, ValueError),
        ('max_request_body_size', 1 << 64, ValueError),
        ('timeout_request_body_idle', True, TypeError),
        ('timeout_request_body_idle', -0.1, ValueError),
        ('timeout_request_body_idle', float('nan'), ValueError),
        ('h2_timeout_response_stall', False, TypeError),
        ('h2_timeout_response_stall', float('inf'), ValueError),
    ],
)
def test_request_policy_uses_config_field_validators(
    field: str, value: object, error: type[Exception]
) -> None:
    values: dict[str, object] = {
        'path': '/upload',
        'max_request_body_size': 4,
        'timeout_request_body_idle': 5.0,
        'h2_timeout_response_stall': 6.0,
    }
    values[field] = value

    with pytest.raises(error, match=field):
        RequestPolicy(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    'table',
    [
        pytest.param(
            {
                'path': '/upload',
                'max_request_body_size': 0,
                'timeout_request_body_idle': 0,
            },
            id='missing',
        ),
        pytest.param(
            {
                'path': '/upload',
                'max_request_body_size': 0,
                'timeout_request_body_idle': 0,
                'h2_timeout_response_stall': 0,
                'extra': True,
            },
            id='unknown',
        ),
    ],
)
def test_config_mapping_requires_exact_request_policy_fields(
    table: dict[str, object],
) -> None:
    with pytest.raises(
        ValueError, match='request_policies entries must contain exactly'
    ):
        Config.from_mapping({'request_policies': [table]})


def test_config_from_env_reads_layered_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('H2CORN_PORT', '9000')
    monkeypatch.setenv('H2CORN_WORKERS', '3')
    monkeypatch.setenv('H2CORN_MAX_REQUESTS', '8')
    monkeypatch.setenv('H2CORN_MAX_REQUESTS_JITTER', '2')
    monkeypatch.setenv('H2CORN_TIMEOUT_WORKER_HEALTHCHECK', '3.5')
    monkeypatch.setenv('H2CORN_HTTP1', 'false')
    monkeypatch.setenv('H2CORN_GRPC', 'true')
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
    monkeypatch.setenv(
        'H2CORN_REQUEST_POLICIES',
        '[{ path = "/example.Chat/*", max_request_body_size = 0, '
        'timeout_request_body_idle = 0, h2_timeout_response_stall = 60.0 }]',
    )
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
    assert config.grpc is True
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
    assert config.request_policies == (
        RequestPolicy(
            path='/example.Chat/*',
            max_request_body_size=0,
            timeout_request_body_idle=0,
            h2_timeout_response_stall=60.0,
        ),
    )
    assert config.websocket_max_message_size == 2048
    assert config.websocket_per_message_deflate is False
    assert config.websocket_ping_interval == 8.5
    assert config.websocket_ping_timeout == 9.5
    assert config.server_header == 'full'
    assert config.date_header is False
    assert config.response_headers == ('x-demo: one', 'x-extra: two')


def test_grpc_admission_is_opt_in_across_configuration_sources() -> None:
    assert Config().grpc is False
    assert Config.from_mapping({'grpc': True}).grpc is True
    assert Config.from_env({'H2CORN_GRPC': 'true'}).grpc is True

    _cli, _import, enabled = parse_cli(['--grpc', 'example:app'], {})
    _cli, _import, disabled = parse_cli(['--no-grpc', 'example:app'], {})

    assert enabled.grpc is True
    assert disabled.grpc is False


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


@pytest.mark.parametrize(
    ('field', 'seconds'),
    [
        ('timeout_request_body_idle', 1e20),
        ('h2_timeout_response_stall', 1e300),
    ],
)
def test_native_config_rejects_unrepresentable_request_policy_durations(
    field: str,
    seconds: float,
) -> None:
    from h2corn._lib import prepare_tls

    values: dict[str, object] = {
        'path': '/upload',
        'max_request_body_size': 0,
        'timeout_request_body_idle': 0.0,
        'h2_timeout_response_stall': 0.0,
    }
    values[field] = seconds
    policy = RequestPolicy(**values)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match=field):
        prepare_tls(Config(request_policies=(policy,)))


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

    assert config.grpc is False
    for option in config_options():
        assert getattr(config, option.name) == option.default


def test_websocket_max_message_size_defaults_to_safe_cap() -> None:
    config = Config()

    assert config.websocket_max_message_size == 16_777_216


def test_request_policy_table_defaults_to_global_body_and_stall_values() -> None:
    config = Config()

    assert config.request_policies == ()
    assert config.max_request_body_size == 1_073_741_824
    assert config.timeout_request_body_idle == 60.0
    assert config.h2_timeout_response_stall == 60.0

    disabled = Config(
        max_request_body_size=0,
        timeout_request_body_idle=0,
        h2_timeout_response_stall=0,
    )
    assert disabled.request_policies == ()
    assert disabled.max_request_body_size == 0
    assert disabled.timeout_request_body_idle == 0.0
    assert disabled.h2_timeout_response_stall == 0.0


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


@pytest.mark.parametrize(
    ('env', 'flags', 'expected_bind'),
    [
        pytest.param(
            {'H2CORN_PORT': '9020'},
            ['--port', '9020'],
            ('192.0.2.1:9020',),
            id='preserve-toml-host',
        ),
        pytest.param(
            {'H2CORN_HOST': '198.51.100.7'},
            ['--host', '198.51.100.7'],
            ('198.51.100.7:9010',),
            id='preserve-toml-port',
        ),
    ],
)
def test_parse_cli_preserves_unoverridden_toml_bind_component(
    tmp_path: Path,
    env: dict[str, str],
    flags: list[str],
    expected_bind: tuple[str, ...],
) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('bind = ["192.0.2.1:9010"]')

    cli_settings, import_settings, env_config = parse_cli(
        ['--config', str(config_path), 'example:app'],
        env,
    )
    _cli_settings, _import_settings, cli_config = parse_cli(
        ['--config', str(config_path), *flags, 'example:app'],
        {},
    )
    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(target='example:app')
    assert env_config.bind == expected_bind
    assert cli_config.bind == expected_bind


def test_request_policy_environment_and_cli_precedence(tmp_path: Path) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
[[request_policies]]
path = "/from-toml"
max_request_body_size = 10
timeout_request_body_idle = 11.0
h2_timeout_response_stall = 12.0
""".strip()
    )
    env = {
        'H2CORN_REQUEST_POLICIES': (
            '[{ path = "/from-env", max_request_body_size = 20, '
            'timeout_request_body_idle = 21.0, h2_timeout_response_stall = 22.0 }]'
        )
    }

    _cli, _import, from_env = parse_cli(
        ['--config', str(config_path), 'example:app'],
        env,
    )
    assert tuple(policy.path for policy in from_env.request_policies) == ('/from-env',)

    _cli, _import, from_cli = parse_cli(
        [
            '--config',
            str(config_path),
            '--request-policy',
            '{ path = "/first", max_request_body_size = 30, timeout_request_body_idle = 31.0, h2_timeout_response_stall = 32.0 }',
            '--request-policy',
            '{ path = "/second", max_request_body_size = 40, timeout_request_body_idle = 41.0, h2_timeout_response_stall = 42.0 }',
            'example:app',
        ],
        env,
    )
    assert tuple(policy.path for policy in from_cli.request_policies) == (
        '/first',
        '/second',
    )


def test_check_config_rejects_incomplete_request_policy_without_target_import(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'incomplete.toml'
    config_path.write_text(
        """
[[request_policies]]
path = "/upload"
max_request_body_size = 0
timeout_request_body_idle = 0
""".strip()
    )

    with pytest.raises(SystemExit) as raised:
        parse_cli(['--check-config', '--config', str(config_path)], {})

    assert raised.value.code == 2


def test_check_config_rejects_unrepresentable_policy_before_importing_target(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from h2corn._cli import run_cli

    with pytest.raises(SystemExit) as raised:
        run_cli(
            lambda *_: pytest.fail('check-config must not load the application'),
            [
                '--check-config',
                '--request-policy',
                '{ path = "/upload", max_request_body_size = 0, timeout_request_body_idle = 1e300, h2_timeout_response_stall = 0 }',
                'missing_application:app',
            ],
            {},
        )

    assert raised.value.code == 2
    assert 'timeout_request_body_idle' in capsys.readouterr().err


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


def test_print_config_round_trips_request_policy_inline_tables(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from h2corn._cli import _print_config

    config = Config(
        request_policies=(
            RequestPolicy(
                path='/example.Chat/*',
                max_request_body_size=0,
                timeout_request_body_idle=0,
                h2_timeout_response_stall=60.0,
            ),
            RequestPolicy(
                path='/upload/%2F',
                max_request_body_size=4096,
                timeout_request_body_idle=2.5,
                h2_timeout_response_stall=0,
            ),
        )
    )

    _print_config(config)

    printed = capsys.readouterr().out
    assert tomllib.loads(printed)['request_policies'] == [
        {
            'path': '/example.Chat/*',
            'max_request_body_size': 0,
            'timeout_request_body_idle': 0.0,
            'h2_timeout_response_stall': 60.0,
        },
        {
            'path': '/upload/%2F',
            'max_request_body_size': 4096,
            'timeout_request_body_idle': 2.5,
            'h2_timeout_response_stall': 0.0,
        },
    ]
    assert Config.from_mapping(tomllib.loads(printed)) == config


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
