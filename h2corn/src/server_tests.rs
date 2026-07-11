use std::io;
use std::iter::repeat_with;
use std::os::fd::{AsRawFd, BorrowedFd};

use rustix::io::{Errno, fcntl_getfd};
use rustix::pipe::pipe;

use super::{ListenerFd, adopt_all};
use crate::config::BindTarget;

fn owned_pipe_writes(count: usize) -> (Box<[ListenerFd]>, Vec<i32>) {
    let mut owned = Vec::with_capacity(count);
    let mut raw = Vec::with_capacity(count);
    for _ in 0..count {
        let (read, write) = pipe().expect("pipe creation succeeds");
        drop(read);
        raw.push(write.as_raw_fd());
        owned.push(write);
    }
    (owned.into_boxed_slice(), raw)
}

fn assert_closed(raw: &[i32]) {
    for &fd in raw {
        // SAFETY: the borrowed descriptor is used only to verify that its
        // RAII owner has closed it; fcntl returns EBADF without dereference.
        let fd = unsafe { BorrowedFd::borrow_raw(fd) };
        assert_eq!(fcntl_getfd(fd).unwrap_err(), Errno::BADF);
    }
}

fn binds(count: usize) -> Vec<BindTarget> {
    repeat_with(|| BindTarget::Tcp {
        host: Box::from("127.0.0.1"),
        port: 0,
    })
    .take(count)
    .collect()
}

#[test]
fn count_mismatch_closes_every_unadopted_handle() {
    let (fds, raw) = owned_pipe_writes(2);
    let result = adopt_all(&binds(1), fds, |_, fd| Ok::<_, io::Error>(fd));
    let _ = result.unwrap_err();
    assert_closed(&raw);
}

#[test]
fn mid_adoption_failure_closes_prior_current_and_remaining_handles() {
    let (fds, raw) = owned_pipe_writes(3);
    let mut index = 0;
    let result = adopt_all(&binds(3), fds, |_, fd| {
        let current = index;
        index += 1;
        if current == 1 {
            return Err(io::Error::other("injected adoption failure"));
        }
        Ok(fd)
    });
    let _ = result.unwrap_err();
    assert_closed(&raw);
}
