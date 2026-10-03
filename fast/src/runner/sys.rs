use std::ffi::CString;
use std::fs::{self, File};
use std::os::fd::{AsRawFd, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;
use std::time::Instant;
use std::{io, ptr};

use rustix::fd::AsFd as _;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use rustix::pipe::{PipeFlags, pipe_with};

pub(super) struct Child {
    pid: i32,
    reaped: bool,
}

pub(super) struct Launch<'a> {
    pub input: i32,
    pub output: i32,
    pub error: i32,
    pub release: i32,
    pub release_write: i32,
    pub exec_error: i32,
    pub mask: &'a libc::sigset_t,
    pub paths: &'a [CString],
    pub argv: &'a [*const libc::c_char],
    pub env: &'a [*const libc::c_char],
}

impl Child {
    pub(super) const fn new(pid: i32) -> Self {
        Self { pid, reaped: false }
    }

    pub(super) fn signal(&self, signal: i32) {
        // SAFETY: the unreaped child reserves its PID/group identity.
        unsafe {
            libc::kill(self.pid.wrapping_neg(), signal);
            libc::kill(self.pid, signal);
        }
    }

    pub(super) fn try_wait(&mut self) -> io::Result<Option<(i32, libc::rusage)>> {
        // SAFETY: wait4 owns reaping for this child, with writable result records.
        unsafe {
            let mut status = 0;
            let mut usage = std::mem::zeroed();
            let result = libc::wait4(self.pid, &raw mut status, libc::WNOHANG, &raw mut usage);
            if result < 0 {
                let error = io::Error::last_os_error();
                return if error.kind() == io::ErrorKind::Interrupted {
                    Ok(None)
                } else {
                    Err(error)
                };
            }
            if result == 0 {
                return Ok(None);
            }
            self.reaped = true;
            Ok(Some((status, usage)))
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if !self.reaped {
            self.signal(libc::SIGKILL);
            // SAFETY: this is the unique reaper on the error/unwind path.
            unsafe {
                while libc::waitpid(self.pid, ptr::null_mut(), 0) < 0 {
                    if *libc::__errno_location() != libc::EINTR {
                        break;
                    }
                }
            }
            drop(kill_adopted());
        }
    }
}

pub(super) fn pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    let (read, write) = pipe_with(PipeFlags::CLOEXEC)?;
    Ok((above_stdio(read)?, above_stdio(write)?))
}

pub(super) fn launch_channel() -> io::Result<(OwnedFd, OwnedFd)> {
    let (child, parent) = std::os::unix::net::UnixStream::pair()?;
    Ok((above_stdio(child.into())?, above_stdio(parent.into())?))
}

pub(super) fn above_stdio(fd: OwnedFd) -> io::Result<OwnedFd> {
    if fd.as_raw_fd() >= 3 {
        return Ok(fd);
    }
    // SAFETY: F_DUPFD_CLOEXEC creates a fresh descriptor at or above three.
    let duplicate = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    cvt(duplicate)?;
    // SAFETY: the successful fcntl returned an owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

pub fn check_jobserver() -> io::Result<()> {
    for key in ["MAKEFLAGS", "CARGO_MAKEFLAGS"] {
        let Some(flags) = std::env::var_os(key) else {
            continue;
        };
        for token in flags.as_bytes().split(u8::is_ascii_whitespace) {
            let Some(auth) = token
                .strip_prefix(b"--jobserver-auth=")
                .or_else(|| token.strip_prefix(b"--jobserver-fds="))
            else {
                continue;
            };
            if auth.starts_with(b"fifo:") {
                continue;
            }
            let descriptors = std::str::from_utf8(auth).map_err(io::Error::other)?;
            let Some((read, write)) = descriptors.split_once(',') else {
                return Err(io::Error::other(format!(
                    "{key} contains invalid jobserver descriptors"
                )));
            };
            for value in <[&str; 2]>::from((read, write)) {
                let descriptor = value.parse::<i32>().map_err(io::Error::other)?;
                // SAFETY: F_GETFD only queries the inherited descriptor.
                let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
                if flags < 0 || flags & libc::FD_CLOEXEC != 0 {
                    return Err(io::Error::other(format!(
                        "{key} refers to unavailable jobserver fd {descriptor}; pass its descriptors or unset {key}"
                    )));
                }
            }
        }
    }
    Ok(())
}
pub(super) fn nonblocking(fd: &OwnedFd) -> io::Result<()> {
    fcntl_setfl(fd, fcntl_getfl(fd)? | OFlags::NONBLOCK)?;
    Ok(())
}

pub(super) fn signals() -> io::Result<(OwnedFd, libc::sigset_t)> {
    // SAFETY: initialized signal sets and valid pointers; the binary is single-threaded.
    unsafe {
        let mut mask = std::mem::zeroed();
        let mut original = std::mem::zeroed();
        libc::sigemptyset(&raw mut mask);
        for signal in [libc::SIGINT, libc::SIGTERM, libc::SIGHUP, libc::SIGCHLD] {
            libc::sigaddset(&raw mut mask, signal);
        }
        cvt(libc::sigprocmask(
            libc::SIG_BLOCK,
            &raw const mask,
            &raw mut original,
        ))?;
        let fd = libc::signalfd(-1, &raw const mask, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK);
        if fd < 0 {
            restore_mask(&original);
            return Err(io::Error::last_os_error());
        }
        match above_stdio(OwnedFd::from_raw_fd(fd)) {
            Ok(descriptor) => Ok((descriptor, original)),
            Err(error) => {
                restore_mask(&original);
                Err(error)
            },
        }
    }
}

pub(super) fn restore_mask(mask: &libc::sigset_t) {
    // SAFETY: mask was obtained from sigprocmask in this single-threaded process.
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, mask, ptr::null_mut());
    }
}

pub(super) fn subreaper() -> io::Result<()> {
    // SAFETY: PR_SET_CHILD_SUBREAPER takes an integer boolean, no pointer.
    cvt(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) })?;
    Ok(())
}

pub(super) fn read_signal(fd: &OwnedFd) -> io::Result<Option<i32>> {
    // SAFETY: the kernel writes at most the supplied initialized record size.
    unsafe {
        let mut info: libc::signalfd_siginfo = std::mem::zeroed();
        let n = libc::read(
            fd.as_raw_fd(),
            (&raw mut info).cast(),
            size_of::<libc::signalfd_siginfo>(),
        );
        if n < 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::WouldBlock {
                Ok(None)
            } else {
                Err(error)
            };
        }
        Ok((n > 0).then_some(info.ssi_signo as i32))
    }
}

pub(super) fn spawn(launch: &Launch<'_>) -> io::Result<i32> {
    // SAFETY: the child uses only preallocated data and async-signal-safe syscalls
    // until exec/_exit. The parent never runs a Rust destructor in the child.
    unsafe {
        let pid = libc::fork();
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }
        if pid == 0 {
            libc::close(launch.release_write);
            let fail = |errno: i32| -> ! {
                libc::write(launch.exec_error, (&raw const errno).cast(), 4);
                libc::_exit(127);
            };
            if libc::setpgid(0, 0) < 0 {
                fail(*libc::__errno_location());
            }
            for (from, to) in [(launch.input, 0), (launch.output, 1), (launch.error, 2)] {
                if libc::dup2(from, to) < 0 {
                    fail(*libc::__errno_location());
                }
            }
            let mut usage: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) == 0 {
                let launch_rss = (usage.ru_maxrss as u64).wrapping_mul(1024).to_ne_bytes();
                if libc::write(launch.release, launch_rss.as_ptr().cast(), launch_rss.len()) != 8 {
                    fail(libc::EPIPE);
                }
            }
            let mut byte = 0_u8;
            loop {
                let n = libc::read(launch.release, (&raw mut byte).cast(), 1);
                if n == 1 {
                    break;
                }
                if n < 0 && *libc::__errno_location() == libc::EINTR {
                    continue;
                }
                fail(libc::EPIPE);
            }
            libc::sigprocmask(libc::SIG_SETMASK, launch.mask, ptr::null_mut());
            libc::signal(libc::SIGPIPE, libc::SIG_DFL);
            let mut denied = false;
            for path in launch.paths {
                libc::execve(path.as_ptr(), launch.argv.as_ptr(), launch.env.as_ptr());
                let errno = *libc::__errno_location();
                match errno {
                    libc::ENOENT | libc::ENOTDIR => {},
                    libc::EACCES => denied = true,
                    _ => fail(errno),
                }
            }
            fail(if denied { libc::EACCES } else { libc::ENOENT });
        }
        Ok(pid)
    }
}

pub(super) fn release(fd: &OwnedFd) -> io::Result<()> {
    rustix::io::write(fd, &[1])?;
    Ok(())
}

pub(super) fn pidfd(pid: i32) -> io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes an integer PID and zero flags, without pointers.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful syscall transfers ownership of a new descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
}

pub(super) fn alive(fd: &OwnedFd) -> bool {
    let mut descriptor = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: a valid pollfd record and zero timeout; no descriptor ownership changes.
    unsafe { libc::poll(&raw mut descriptor, 1, 0) == 0 }
}

pub(super) fn signal_pidfd(fd: &OwnedFd, signal: i32) {
    // SAFETY: pidfd identifies the owned process; null siginfo selects ordinary delivery.
    unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            signal,
            ptr::null::<libc::siginfo_t>(),
            0,
        );
    }
}

pub(super) fn reap_descendants() -> io::Result<bool> {
    loop {
        // SAFETY: the root is already reaped; all remaining children are adopted descendants.
        let pid = unsafe { libc::waitpid(-1, ptr::null_mut(), libc::WNOHANG) };
        if pid > 0 {
            continue;
        }
        if pid == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ECHILD) {
            return Ok(false);
        }
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub(super) fn kill_adopted() -> io::Result<()> {
    // Reparenting after each kill exposes the next generation without a host scan.
    let deadline = Instant::now()
        .checked_add(std::time::Duration::from_secs(1))
        .expect("one-second deadline");
    while reap_descendants()? && Instant::now() < deadline {
        let text = fs::read_to_string("/proc/thread-self/children")?;
        for pid in text.split_whitespace().filter_map(|s| s.parse().ok()) {
            if let Ok(fd) = pidfd(pid) {
                signal_pidfd(&fd, libc::SIGKILL);
                poll_one(fd.as_raw_fd(), Some(deadline))?;
            }
        }
    }
    Ok(())
}

pub(super) fn poll(
    pid: &OwnedFd,
    signals: &OwnedFd,
    stderr: Option<&File>,
    deadline: Option<Instant>,
) -> io::Result<()> {
    let mut fds = [
        libc::pollfd {
            fd: pid.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: signals.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: stderr.map_or(-1, AsRawFd::as_raw_fd),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    poll_fds(&mut fds, deadline)
}

pub(super) fn poll_pipe(pipe: &File, deadline: Instant) -> io::Result<()> {
    poll_one(pipe.as_fd().as_raw_fd(), Some(deadline))
}

fn poll_one(fd: i32, deadline: Option<Instant>) -> io::Result<()> {
    poll_fds(
        &mut [libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        }],
        deadline,
    )
}

fn poll_fds(fds: &mut [libc::pollfd], deadline: Option<Instant>) -> io::Result<()> {
    let duration = deadline.map(|end| end.saturating_duration_since(Instant::now()));
    let timeout = duration.map(|d| libc::timespec {
        tv_sec: d.as_secs() as i64,
        tv_nsec: i64::from(d.subsec_nanos()),
    });
    // SAFETY: valid pollfd slice and optional timespec, no signal-mask change.
    let result = unsafe {
        libc::ppoll(
            fds.as_mut_ptr(),
            fds.len() as libc::nfds_t,
            timeout.as_ref().map_or(ptr::null(), ptr::from_ref),
            ptr::null(),
        )
    };
    if result < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(super) const fn timeval_ns(time: libc::timeval) -> u64 {
    (time.tv_sec as u64)
        .wrapping_mul(1_000_000_000)
        .wrapping_add((time.tv_usec as u64).wrapping_mul(1000))
}

pub(super) fn executable(path: &Path) -> bool {
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: NUL-terminated path and a valid access mode.
    unsafe { libc::access(path.as_ptr(), libc::X_OK) == 0 }
}

fn cvt(value: i32) -> io::Result<i32> {
    if value < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(value)
    }
}
