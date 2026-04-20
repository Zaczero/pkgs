import socket
import sys
from pathlib import Path
from typing import Any

import pytest
from h2corn import Config


def _recording_socket(
    calls: list[tuple[str, tuple[Any, ...]]],
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

    _socket._build_sockets(config)[0]

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

    _socket._build_sockets(config)[0]

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

    _socket._build_sockets(config)[0]

    assert calls[0] == ('socket', (_socket.socket.AF_INET, _socket.socket.SOCK_STREAM))
    assert ('setblocking', (False,)) in calls
    assert ('listen', (config.backlog,)) in calls


def test_import_target_requires_module_app_form() -> None:
    from h2corn import _server

    with pytest.raises(ValueError, match='module:app form'):
        _server._import_target('demoapp')


def test_import_target_requires_callable_attribute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _server

    module_name = 'demoapp_not_callable'
    (tmp_path / f'{module_name}.py').write_text('app = 1\n')
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(
        TypeError,
        match=rf"import target '{module_name}:app' is not callable",
    ):
        _server._import_target(f'{module_name}:app')


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

    _socket._build_sockets(config)[0]

    assert ('bind', (('127.0.0.1', 0),)) in calls
    assert config.port == 54_321


def test_build_sockets_reuses_kernel_allocated_port_across_tcp_zero_binds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    config = Config(bind=('127.0.0.1:0', '[::1]:0'))
    calls = []
    ports = iter([54_321, 54_321])

    class FakeSocket:
        def __init__(self, family: int) -> None:
            self.family = family
            self._port = next(ports)

        def setsockopt(self, *args) -> None:
            calls.append(('setsockopt', args))

        def bind(self, *args) -> None:
            calls.append(('bind', args))

        def listen(self, *args) -> None:
            calls.append(('listen', args))

        def setblocking(self, *args) -> None:
            calls.append(('setblocking', args))

        def getsockname(self):
            host = '::1' if self.family == _socket.socket.AF_INET6 else '127.0.0.1'
            return (host, self._port, 0, 0) if self.family == _socket.socket.AF_INET6 else (host, self._port)

        def close(self) -> None:
            calls.append(('close', (self.family,)))

    monkeypatch.setattr(_socket.sys, 'platform', 'linux')
    monkeypatch.setattr(
        _socket.socket,
        'getaddrinfo',
        lambda host, port, **_kwargs: [
            (
                _socket.socket.AF_INET6 if ':' in host else _socket.socket.AF_INET,
                _socket.socket.SOCK_STREAM,
                0,
                '',
                (host, port, 0, 0) if ':' in host else (host, port),
            )
        ],
    )
    monkeypatch.setattr(_socket.socket, 'socket', lambda family, *_args: FakeSocket(family))

    sockets = _socket._build_sockets(config)

    assert len(sockets) == 2
    assert ('bind', (('127.0.0.1', 0),)) in calls
    assert ('bind', (('::1', 54321, 0, 0),)) in calls
    assert config.bind == ('127.0.0.1:54321', '[::1]:54321')


@pytest.mark.parametrize(
    ('family', 'is_unix'),
    [
        (socket.AF_INET, False),
        pytest.param(
            socket.AF_UNIX,
            True,
            marks=pytest.mark.skipif(
                sys.platform == 'win32',
                reason='unix sockets are not supported on Windows',
            ),
        ),
    ],
)
def test_build_sockets_records_fd_listener_family(
    monkeypatch: pytest.MonkeyPatch,
    family: int,
    is_unix: bool,
) -> None:
    from h2corn import _socket

    class FakeSocket:
        def __init__(self) -> None:
            self.family = family

        def setblocking(self, _blocking: bool) -> None:
            pass

    config = Config(bind=('fd://7',))

    def fake_socket(*, fileno: int):
        assert fileno == 7
        return FakeSocket()

    monkeypatch.setattr(_socket.socket, 'socket', fake_socket)

    sockets = _socket._build_sockets(config)

    assert len(sockets) == 1
    assert config.bind == ('fd://7',)
    assert config._bind_fd_is_unix == (is_unix,)


def test_build_sockets_rolls_back_open_listeners_on_partial_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from h2corn import _socket

    calls = []
    sockets = []

    class FakeSocket:
        def __init__(self, family: int) -> None:
            self.family = family
            sockets.append(self)

        def setsockopt(self, *args) -> None:
            calls.append(('setsockopt', args))

        def bind(self, sockaddr) -> None:
            calls.append(('bind', (sockaddr,)))
            if sockaddr == ('127.0.0.1', 8001):
                raise OSError('bind failed')

        def listen(self, *args) -> None:
            calls.append(('listen', args))

        def getsockname(self) -> tuple[str, int]:
            return ('127.0.0.1', 8000)

        def close(self) -> None:
            calls.append(('close', (self.family,)))

    monkeypatch.setattr(_socket.sys, 'platform', 'linux')
    monkeypatch.setattr(
        _socket.socket,
        'getaddrinfo',
        lambda host, port, **_kwargs: [
            (_socket.socket.AF_INET, _socket.socket.SOCK_STREAM, 0, '', (host, port))
        ],
    )
    monkeypatch.setattr(_socket.socket, 'socket', lambda family, *_args: FakeSocket(family))

    with pytest.raises(OSError, match='bind failed'):
        _socket._build_sockets(Config(bind=('127.0.0.1:8000', '127.0.0.1:8001')))

    assert calls.count(('close', (_socket.socket.AF_INET,))) == 2


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


def test_cli_repeated_bind_replaces_base_bind_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('bind = ["127.0.0.1:9010"]')
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
            '--bind',
            '127.0.0.1:9030',
            '--bind',
            'unix:/tmp/h2corn.sock',
            'example:app',
        ],
    )

    _server.main()

    assert captured['config'].bind == ('127.0.0.1:9030', 'unix:/tmp/h2corn.sock')


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


def test_cli_legacy_env_port_overrides_toml_listener(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from h2corn import _server

    config_path = tmp_path / 'h2corn.toml'
    config_path.write_text('port = 9010')
    captured = {}

    monkeypatch.setenv('H2CORN_PORT', '9020')
    monkeypatch.setattr(_server, '_import_target', lambda _target: object())
    monkeypatch.setattr(
        _server,
        'serve',
        lambda _app, config=None: captured.setdefault('config', config),
    )
    monkeypatch.setattr(
        sys,
        'argv',
        ['h2corn', '--config', str(config_path), 'example:app'],
    )

    _server.main()

    assert captured['config'].port == 9020
