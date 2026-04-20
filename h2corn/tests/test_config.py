import argparse
import os
from pathlib import Path

import pytest
from h2corn import Config
from h2corn._cli import CliSettings, ImportSettings, build_parser, parse_cli
from h2corn._config import config_options


@pytest.mark.parametrize(
    ('kwargs', 'match'),
    [
        ({'backlog': 0}, 'backlog'),
        ({'workers': 0}, 'workers'),
        ({'runtime_threads': 0}, 'runtime_threads'),
        ({'user': ''}, 'user'),
        ({'group': ''}, 'group'),
        ({'umask': -1}, 'umask'),
        ({'umask': 0o1000}, 'umask'),
        ({'limit_request_line': -1}, 'limit_request_line'),
        ({'limit_request_head_size': -1}, 'limit_request_head_size'),
        ({'limit_request_fields': -1}, 'limit_request_fields'),
        ({'limit_request_field_size': -1}, 'limit_request_field_size'),
        ({'timeout_graceful_shutdown': -1}, 'timeout_graceful_shutdown'),
        ({'max_concurrent_streams': -1}, 'max_concurrent_streams'),
        ({'max_concurrent_streams': 4_294_967_296}, 'max_concurrent_streams'),
        ({'h2_max_header_list_size': -1}, 'h2_max_header_list_size'),
        ({'h2_max_header_list_size': 4_294_967_296}, 'h2_max_header_list_size'),
        ({'h2_max_header_block_size': -1}, 'h2_max_header_block_size'),
        ({'h2_max_inbound_frame_size': 16_383}, 'h2_max_inbound_frame_size'),
        ({'max_request_body_size': -1}, 'max_request_body_size'),
        ({'timeout_handshake': -1}, 'timeout_handshake'),
        ({'websocket_max_message_size': -1}, 'websocket_max_message_size'),
    ],
)
def test_config_rejects_invalid_numeric_values(
    kwargs: dict[str, int | float],
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        Config(**kwargs)


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
user = "www-data"
group = "www-data"
umask = "027"
proxy_headers = true
forwarded_allow_ips = ["127.0.0.1"]
timeout_keep_alive = 1.5
timeout_request_header = 2.5
timeout_request_body_idle = 3.5
limit_concurrency = 9
limit_connections = 11
runtime_threads = 4
timeout_lifespan_startup = 6.5
timeout_lifespan_shutdown = 7.5
websocket_per_message_deflate = false
websocket_ping_interval = 8.5
websocket_ping_timeout = 9.5
server_header = true
date_header = false
response_headers = ["x-demo: one", "x-extra: two"]
""".strip()
    )

    config = Config.from_toml(config_path)

    assert config.port == 9010
    assert config.workers == 2
    assert config.max_requests == 11
    assert config.max_requests_jitter == 3
    assert config.timeout_worker_healthcheck == 4.5
    assert config.http1 is False
    assert config.pid == Path('server.pid')
    assert config.user == 'www-data'
    assert config.group == 'www-data'
    assert config.umask == 0o27
    assert config.proxy_headers is True
    assert config.timeout_keep_alive == 1.5
    assert config.timeout_request_header == 2.5
    assert config.timeout_request_body_idle == 3.5
    assert config.limit_concurrency == 9
    assert config.limit_connections == 11
    assert config.runtime_threads == 4
    assert config.lifespan == 'on'
    assert config.timeout_lifespan_startup == 6.5
    assert config.timeout_lifespan_shutdown == 7.5
    assert config.websocket_per_message_deflate is False
    assert config.websocket_ping_interval == 8.5
    assert config.websocket_ping_timeout == 9.5
    assert config.server_header is True
    assert config.date_header is False
    assert config.response_headers == ('x-demo: one', 'x-extra: two')


def test_config_from_toml_accepts_websocket_message_size_inherit(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('websocket_max_message_size = "inherit"')

    config = Config.from_toml(config_path)

    assert config.websocket_max_message_size is None


def test_config_from_env_reads_layered_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('H2CORN_PORT', '9000')
    monkeypatch.setenv('H2CORN_WORKERS', '3')
    monkeypatch.setenv('H2CORN_MAX_REQUESTS', '8')
    monkeypatch.setenv('H2CORN_MAX_REQUESTS_JITTER', '2')
    monkeypatch.setenv('H2CORN_TIMEOUT_WORKER_HEALTHCHECK', '3.5')
    monkeypatch.setenv('H2CORN_HTTP1', 'false')
    monkeypatch.setenv('H2CORN_PID', 'server.pid')
    monkeypatch.setenv('H2CORN_USER', 'www-data')
    monkeypatch.setenv('H2CORN_GROUP', 'www-data')
    monkeypatch.setenv('H2CORN_UMASK', '027')
    monkeypatch.setenv('H2CORN_ACCESS_LOG', 'false')
    monkeypatch.setenv('H2CORN_PROXY_HEADERS', 'true')
    monkeypatch.setenv('H2CORN_FORWARDED_ALLOW_IPS', '127.0.0.1,unix')
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
    monkeypatch.setenv('H2CORN_SERVER_HEADER', 'true')
    monkeypatch.setenv('H2CORN_DATE_HEADER', 'false')
    monkeypatch.setenv('H2CORN_RESPONSE_HEADERS', 'x-demo: one,x-extra: two')

    config = Config.from_env(os.environ)

    assert config.port == 9000
    assert config.workers == 3
    assert config.max_requests == 8
    assert config.max_requests_jitter == 2
    assert config.timeout_worker_healthcheck == 3.5
    assert config.http1 is False
    assert config.pid == Path('server.pid')
    assert config.user == 'www-data'
    assert config.group == 'www-data'
    assert config.umask == 0o27
    assert config.access_log is False
    assert config.proxy_headers is True
    assert config.forwarded_allow_ips == ('127.0.0.1', 'unix')
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
    assert config.server_header is True
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


def test_config_from_env_accepts_websocket_message_size_inherit() -> None:
    config = Config.from_env({'H2CORN_WEBSOCKET_MAX_MESSAGE_SIZE': 'inherit'})

    assert config.websocket_max_message_size is None


def test_config_rejects_empty_unix_bind_path() -> None:
    with pytest.raises(ValueError, match='invalid unix bind target'):
        Config(bind=('unix:',))


def test_config_rejects_invalid_trusted_proxy_entry() -> None:
    with pytest.raises(ValueError, match='forwarded_allow_ips'):
        Config(forwarded_allow_ips=('example.invalid',))


def test_config_allows_empty_trusted_proxy_set() -> None:
    config = Config(forwarded_allow_ips=())

    assert config.forwarded_allow_ips == ()


def test_config_normalizes_multiple_bind_entries() -> None:
    config = Config(bind=['127.0.0.1:8000', '[::1]:8000', 'unix:/tmp/h2corn.sock'])

    assert config.bind == (
        '127.0.0.1:8000',
        '[::1]:8000',
        'unix:/tmp/h2corn.sock',
    )
    assert config.host is None
    assert config.port is None


def test_config_rejects_ping_timeout_without_interval() -> None:
    with pytest.raises(ValueError, match='websocket_ping_timeout'):
        Config(websocket_ping_interval=0.0, websocket_ping_timeout=1.0)


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
    assert len({option.toml_key for option in options}) == len(options)
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
    assert config.port == 9020


def test_parse_cli_accepts_websocket_message_size_inherit() -> None:
    cli_settings, import_settings, config = parse_cli(
        ['--websocket-max-message-size', 'inherit', 'example:app'],
        {},
    )

    assert cli_settings == CliSettings()
    assert import_settings == ImportSettings(target='example:app')
    assert config.websocket_max_message_size is None


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


def test_parse_cli_version_exits_without_target(capsys: pytest.CaptureFixture[str]) -> None:
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
