import sys
from pathlib import Path

import pytest
from h2corn import Config


def _recording_socket(
    calls: list[tuple[str, tuple[object, ...]]],
    *,
    fail_tcp_defer_accept: bool = False,
    bound_port: int = 43_210,
):
    from h2corn import _socket

    class FakeSocket:
        def setsockopt(self, *args) -> None:
            calls.append(('setsockopt', args))
            if fail_tcp_defer_accept and args == (
                _socket.socket.IPPROTO_TCP,
                getattr(_socket.socket, 'TCP_DEFER_ACCEPT', 9),
                1,
            ):
                raise OSError('unsupported')

        def bind(self, *args) -> None:
            calls.append(('bind', args))

        def listen(self, *args) -> None:
            calls.append(('listen', args))

        def setblocking(self, *args) -> None:
            calls.append(('setblocking', args))

        def getsockname(self) -> tuple[str, int]:
            return ('127.0.0.1', bound_port)

    return FakeSocket()


def test_build_socket_sets_tcp_defer_accept_for_tcp_on_linux(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    config = Config()
    calls = []

    monkeypatch.setattr(_socket.sys, 'platform', 'linux')
    monkeypatch.setattr(_socket.socket, 'TCP_DEFER_ACCEPT', 9, raising=False)
    monkeypatch.setattr(
        _socket.socket,
        'socket',
        lambda *args: calls.append(('socket', args)) or _recording_socket(calls),
    )

    _socket._build_socket(config)

    assert calls[0] == (
        'socket',
        (
            _socket.socket.AF_INET,
            _socket.socket.SOCK_STREAM | _socket.socket.SOCK_NONBLOCK,
        ),
    )
    assert (
        'setsockopt',
        (_socket.socket.SOL_SOCKET, _socket.socket.SO_REUSEADDR, 1),
    ) in calls
    assert ('setsockopt', (_socket.socket.IPPROTO_TCP, 9, 1)) in calls
    assert ('listen', (config.backlog,)) in calls
    assert ('setblocking', (False,)) not in calls


def test_build_socket_ignores_tcp_defer_accept_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    config = Config()
    calls = []

    monkeypatch.setattr(_socket.sys, 'platform', 'linux')
    monkeypatch.setattr(_socket.socket, 'TCP_DEFER_ACCEPT', 9, raising=False)
    monkeypatch.setattr(
        _socket.socket,
        'socket',
        lambda *_args: _recording_socket(calls, fail_tcp_defer_accept=True),
    )

    _socket._build_socket(config)

    assert ('setsockopt', (_socket.socket.IPPROTO_TCP, 9, 1)) in calls
    assert ('listen', (config.backlog,)) in calls
    assert ('setblocking', (False,)) not in calls


def test_build_socket_sets_nonblocking_after_creation_off_linux(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    config = Config()
    calls = []

    monkeypatch.setattr(_socket.sys, 'platform', 'darwin')
    monkeypatch.delattr(_socket.socket, 'SOCK_NONBLOCK', raising=False)
    monkeypatch.setattr(
        _socket.socket,
        'socket',
        lambda *args: calls.append(('socket', args)) or _recording_socket(calls),
    )

    _socket._build_socket(config)

    assert calls[0] == ('socket', (_socket.socket.AF_INET, _socket.socket.SOCK_STREAM))
    assert ('setblocking', (False,)) in calls
    assert ('listen', (config.backlog,)) in calls


def test_build_socket_records_kernel_allocated_port_when_requested_port_is_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    config = Config(port=0)
    calls = []

    monkeypatch.setattr(_socket.sys, 'platform', 'linux')
    monkeypatch.setattr(
        _socket.socket,
        'socket',
        lambda *_args: _recording_socket(calls, bound_port=54_321),
    )

    _socket._build_socket(config)

    assert ('bind', (('127.0.0.1', 0),)) in calls
    assert config.port == 54_321


def test_signal_wakeup_pipe_uses_pipe2_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    calls = []

    monkeypatch.setattr(_socket.sys, 'platform', 'linux')
    monkeypatch.setattr(
        _socket.os, 'pipe2', lambda flags: calls.append(('pipe2', (flags,))) or (11, 12)
    )
    monkeypatch.setattr(_socket.os, 'close', lambda fd: calls.append(('close', (fd,))))
    monkeypatch.setattr(
        _socket.signal,
        'set_wakeup_fd',
        lambda fd, warn_on_full_buffer=False: (
            calls.append(
                ('set_wakeup_fd', (fd, warn_on_full_buffer)),
            )
            or -1
        ),
    )

    with _socket._signal_wakeup_pipe() as read_fd:
        assert read_fd == 11

    assert calls[:2] == [
        ('pipe2', (_socket.os.O_NONBLOCK,)),
        ('set_wakeup_fd', (12, False)),
    ]


def test_signal_wakeup_pipe_falls_back_when_pipe2_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    calls = []

    monkeypatch.setattr(_socket.sys, 'platform', 'darwin')
    monkeypatch.delattr(_socket.os, 'pipe2', raising=False)
    monkeypatch.setattr(
        _socket.os, 'pipe', lambda: calls.append(('pipe', ())) or (21, 22)
    )
    monkeypatch.setattr(_socket.os, 'close', lambda fd: calls.append(('close', (fd,))))
    monkeypatch.setattr(
        _socket.os,
        'set_blocking',
        lambda fd, blocking: calls.append(('set_blocking', (fd, blocking))),
    )
    monkeypatch.setattr(
        _socket.signal,
        'set_wakeup_fd',
        lambda fd, warn_on_full_buffer=False: (
            calls.append(
                ('set_wakeup_fd', (fd, warn_on_full_buffer)),
            )
            or -1
        ),
    )

    with _socket._signal_wakeup_pipe() as read_fd:
        assert read_fd == 21

    assert calls[:4] == [
        ('pipe', ()),
        ('set_blocking', (21, False)),
        ('set_blocking', (22, False)),
        ('set_wakeup_fd', (22, False)),
    ]


def test_cli_trusted_proxy_flags_replace_base_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
forwarded_allow_ips = ["127.0.0.1", "::1"]
""".strip()
    )
    captured = {}

    monkeypatch.setattr(_server, '_import_target', lambda _target: object())
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--config',
            str(config_path),
            '--forwarded-allow-ips',
            '10.0.0.1, unix',
            'example:app',
        ],
    )

    _server.main()

    assert captured['config'].forwarded_allow_ips == ('10.0.0.1', 'unix')


def test_cli_arguments_override_env_and_toml_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text(
        """
port = 9010
http1 = false
access_log = false
""".strip()
    )
    captured = {}

    monkeypatch.setenv('H2CORN_PORT', '9020')
    monkeypatch.setenv('H2CORN_HTTP1', 'false')
    monkeypatch.setenv('H2CORN_ACCESS_LOG', 'false')
    monkeypatch.setattr(_server, '_import_target', lambda _target: object())
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        [
            'h2corn',
            '--config',
            str(config_path),
            '--port',
            '9030',
            '--http1',
            '--access-log',
            'example:app',
        ],
    )

    _server.main()

    assert captured['config'].port == 9030
    assert captured['config'].http1 is True
    assert captured['config'].access_log is True
