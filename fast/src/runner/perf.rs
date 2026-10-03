use std::fs::File;
use std::io::{self, Read as _};
use std::os::fd::FromRawFd as _;

use perf_event_open_sys::{bindings as perf, perf_event_open};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Policy {
    pub events: Vec<Event>,
    pub scope: &'static str,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Event {
    pub name: &'static str,
    pub event_type: u32,
    pub config: u64,
}

#[derive(Debug, Serialize)]
pub struct Counter {
    pub name: &'static str,
    #[serde(serialize_with = "optional_decimal")]
    pub raw: Option<u64>,
    #[serde(serialize_with = "optional_decimal")]
    pub enabled_ns: Option<u64>,
    #[serde(serialize_with = "optional_decimal")]
    pub running_ns: Option<u64>,
    pub unavailable: Option<String>,
}

impl Counter {
    pub fn fully_scheduled(&self) -> Option<u64> {
        let running = self.running_ns?;
        (running > 0 && running == self.enabled_ns?).then_some(self.raw?)
    }

    pub fn usable(&self) -> Option<f64> {
        let enabled = self.enabled_ns?;
        let running = self.running_ns?;
        if running == 0 || (running as f64) / (enabled as f64) < 0.95 {
            return None;
        }
        Some(self.raw? as f64 * enabled as f64 / running as f64)
    }
}

pub(super) struct Events(Vec<(&'static str, Result<File, String>)>);

impl Policy {
    pub(super) fn new() -> Self {
        let specifications = [
            ("instructions", perf::PERF_COUNT_HW_INSTRUCTIONS),
            ("cycles", perf::PERF_COUNT_HW_CPU_CYCLES),
            ("branch-misses", perf::PERF_COUNT_HW_BRANCH_MISSES),
        ]
        .into_iter()
        .map(|(name, config)| (name, perf::PERF_TYPE_HARDWARE, u64::from(config)));
        // Zen 3 core-to-L2 request misses exclude L2 prefetch requests.
        let cache = is_zen3_vermeer().then_some(("l2-misses", perf::PERF_TYPE_RAW, 0x0964));
        let events = specifications
            .chain(cache)
            .map(|(name, event_type, config)| Event {
                name,
                event_type,
                config,
            })
            .collect();
        Self {
            events,
            scope: "user-space root and inherited descendants; enabled on root exec; separate ungrouped events",
        }
    }

    pub(super) fn attach(&self, pid: i32) -> Events {
        Events(
            self.events
                .iter()
                .map(|event| {
                    (
                        event.name,
                        open(pid, event.event_type, event.config).map_err(|e| e.to_string()),
                    )
                })
                .collect(),
        )
    }
}

impl Events {
    pub(super) fn finish(self) -> Vec<Counter> {
        self.0
            .into_iter()
            .map(|(name, handle)| {
                let reading = handle.and_then(|mut file| {
                    let mut bytes = [0_u8; 24];
                    file.read_exact(&mut bytes).map_err(|e| e.to_string())?;
                    let mut words = bytes
                        .as_chunks::<8>()
                        .0
                        .iter()
                        .map(|&chunk| u64::from_ne_bytes(chunk));
                    Ok((words.next(), words.next(), words.next()))
                });
                match reading {
                    Ok((raw, enabled_ns, running_ns)) => Counter {
                        name,
                        raw,
                        enabled_ns,
                        running_ns,
                        unavailable: None,
                    },
                    Err(reason) => Counter {
                        name,
                        raw: None,
                        enabled_ns: None,
                        running_ns: None,
                        unavailable: Some(reason),
                    },
                }
            })
            .collect()
    }
}

#[expect(
    clippy::ref_option,
    clippy::trivially_copy_pass_by_ref,
    reason = "Serde serialize_with requires this exact reference signature"
)]
fn optional_decimal<S: serde::Serializer>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value.map(|v| v.to_string()).serialize(serializer)
}

fn is_zen3_vermeer() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        let (vendor, identity) = (std::arch::x86_64::__cpuid(0), std::arch::x86_64::__cpuid(1));
        let amd =
            vendor.ebx == 0x6874_7541 && vendor.edx == 0x6974_6E65 && vendor.ecx == 0x444D_4163;
        let family_19h = identity.eax & 0x0FF0_0F00 == 0x00A0_0F00;
        let model_21h = identity.eax & 0x000F_00F0 == 0x0002_0010;
        amd && family_19h && model_21h
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

fn open(pid: i32, event_type: u32, config: u64) -> io::Result<File> {
    let mut attr = perf::perf_event_attr {
        type_: event_type,
        size: size_of::<perf::perf_event_attr>() as u32,
        config,
        read_format: u64::from(
            perf::PERF_FORMAT_TOTAL_TIME_ENABLED | perf::PERF_FORMAT_TOTAL_TIME_RUNNING,
        ),
        ..Default::default()
    };
    attr.set_disabled(1);
    attr.set_enable_on_exec(1);
    attr.set_inherit(1);
    attr.set_exclude_kernel(1);
    attr.set_exclude_hv(1);
    // SAFETY: fully initialized UAPI record; counting only, without mmap or sampling.
    let fd = unsafe {
        perf_event_open(
            &raw mut attr,
            pid,
            -1,
            -1,
            u64::from(perf::PERF_FLAG_FD_CLOEXEC),
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: perf_event_open returned a fresh owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(test)]
mod tests {
    use super::Counter;

    #[test]
    fn multiplexed_counts_cannot_supply_full_window_ratios() {
        let mut counter = Counter {
            name: "instructions",
            raw: Some(970),
            enabled_ns: Some(100),
            running_ns: Some(97),
            unavailable: None,
        };
        assert_eq!(counter.usable(), Some(1000.0));
        assert_eq!(counter.fully_scheduled(), None);
        counter.running_ns = Some(100);
        assert_eq!(counter.fully_scheduled(), Some(970));
        counter.running_ns = Some(0);
        assert_eq!(counter.usable(), None);
        assert_eq!(counter.fully_scheduled(), None);
    }
}
