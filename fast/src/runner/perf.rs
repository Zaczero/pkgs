use std::fs::File;
use std::io::{self, Read as _};
use std::os::fd::FromRawFd as _;

use perf_event_open_sys::{bindings as perf, perf_event_open};
use serde::ser::SerializeStruct as _;
use serde::{Serialize, Serializer};

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

#[derive(Debug)]
pub struct Counter {
    pub name: &'static str,
    pub reading: Result<Reading, String>,
}

/// One complete kernel counter reading, including its scheduling window.
#[derive(Clone, Copy, Debug)]
pub struct Reading {
    pub raw: u64,
    pub enabled_ns: u64,
    pub running_ns: u64,
}

impl Counter {
    pub fn fully_scheduled(&self) -> Option<u64> {
        let reading = self.reading.as_ref().ok()?;
        (reading.running_ns > 0 && reading.running_ns == reading.enabled_ns).then_some(reading.raw)
    }

    pub fn usable(&self) -> Option<f64> {
        let reading = self.reading.as_ref().ok()?;
        let enabled = reading.enabled_ns;
        let running = reading.running_ns;
        if running == 0 || (running as f64) / (enabled as f64) < 0.95 {
            return None;
        }
        Some(reading.raw as f64 * enabled as f64 / running as f64)
    }
}

impl Serialize for Counter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let reading = self.reading.as_ref().ok();
        let mut fields = serializer.serialize_struct("Counter", 5)?;
        fields.serialize_field("name", self.name)?;
        fields.serialize_field("raw", &reading.map(|value| value.raw.to_string()))?;
        fields.serialize_field(
            "enabled_ns",
            &reading.map(|value| value.enabled_ns.to_string()),
        )?;
        fields.serialize_field(
            "running_ns",
            &reading.map(|value| value.running_ns.to_string()),
        )?;
        fields.serialize_field("unavailable", &self.reading.as_ref().err())?;
        fields.end()
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
                    let [raw, enabled_ns, running_ns] = bytes.as_chunks::<8>().0 else {
                        unreachable!("a complete counter read contains three u64 words")
                    };
                    Ok(Reading {
                        raw: u64::from_ne_bytes(*raw),
                        enabled_ns: u64::from_ne_bytes(*enabled_ns),
                        running_ns: u64::from_ne_bytes(*running_ns),
                    })
                });
                Counter { name, reading }
            })
            .collect()
    }
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
    use super::{Counter, Reading};

    #[test]
    fn counter_json_preserves_complete_and_unavailable_readings() {
        let mut counter = Counter {
            name: "instructions",
            reading: Ok(Reading {
                raw: u64::MAX,
                enabled_ns: 100,
                running_ns: 97,
            }),
        };
        assert_eq!(
            serde_json::to_value(&counter).unwrap(),
            serde_json::json!({
                "name": "instructions",
                "raw": "18446744073709551615",
                "enabled_ns": "100",
                "running_ns": "97",
                "unavailable": null,
            })
        );
        counter.reading = Err("permission denied".into());
        assert_eq!(
            serde_json::to_value(&counter).unwrap(),
            serde_json::json!({
                "name": "instructions",
                "raw": null,
                "enabled_ns": null,
                "running_ns": null,
                "unavailable": "permission denied",
            })
        );
        assert_eq!(counter.usable(), None);
        assert_eq!(counter.fully_scheduled(), None);
    }

    #[test]
    fn multiplexed_counts_cannot_supply_full_window_ratios() {
        let mut counter = Counter {
            name: "instructions",
            reading: Ok(Reading {
                raw: 970,
                enabled_ns: 100,
                running_ns: 97,
            }),
        };
        assert_eq!(counter.usable(), Some(1000.0));
        assert_eq!(counter.fully_scheduled(), None);
        counter.reading.as_mut().unwrap().running_ns = 100;
        assert_eq!(counter.fully_scheduled(), Some(970));
        counter.reading.as_mut().unwrap().running_ns = 0;
        assert_eq!(counter.usable(), None);
        assert_eq!(counter.fully_scheduled(), None);
    }
}
