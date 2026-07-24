//! bcast-probe — an iperf-style diagnostic for the udp multicast medium.
//!
//! Answers, in seconds and without involving a conductor: does multicast
//! actually pass on this LAN, and at what loss rate? Run it on two (or
//! more) machines:
//!
//! ```sh
//! # machine A: beacon 10 frames/sec of 1200 bytes and listen
//! cargo run -p kitsune2_transport_broadcast --example bcast_probe -- --send 10
//!
//! # machine B: just listen
//! cargo run -p kitsune2_transport_broadcast --example bcast_probe
//! ```
//!
//! Each report line shows, per sender heard, the received/expected frame
//! count derived from sequence numbers (= loss), the data rate, and how
//! recently it was heard. Your own beacons show up as a `self` row via
//! multicast loopback, which confirms the local transmit path
//! independently of the network.
//!
//! Probe frames use their own magic and are ignored by the broadcast
//! transport (and vice versa), so probing can safely share the group and
//! port with live nodes.

use futures::StreamExt;
use kitsune2_transport_broadcast::{UdpMulticastConfig, UdpMulticastMedium};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAGIC: &[u8; 8] = b"K2PROBE1";
const HEADER_LEN: usize = 8 + 8 + 8; // magic + sender id + seq

struct Args {
    group: String,
    port: u16,
    mtu: usize,
    send_hz: Option<f64>,
    size: usize,
    report_secs: u64,
}

fn usage() -> ! {
    eprintln!(
        "bcast-probe: LAN multicast diagnostic for the kitsune2 broadcast \
         transport\n\n\
         USAGE:\n  bcast_probe [OPTIONS]\n\n\
         OPTIONS:\n\
         \x20 --group <ADDR>    multicast group [default: 239.19.42.7]\n\
         \x20 --port <PORT>     shared udp port [default: 24842]\n\
         \x20 --mtu <BYTES>     max frame size [default: 1400]\n\
         \x20 --send <HZ>       also beacon numbered frames at this rate\n\
         \x20 --size <BYTES>    beacon frame size [default: 1200]\n\
         \x20 --report <SECS>   report interval [default: 2]\n\
         \x20 --help            show this help"
    );
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut out = Args {
        group: "239.19.42.7".into(),
        port: 24842,
        mtu: 1400,
        send_hz: None,
        size: 1200,
        report_secs: 2,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = |name: &str| {
            args.next().unwrap_or_else(|| {
                eprintln!("missing value for {name}");
                usage()
            })
        };
        match arg.as_str() {
            "--group" => out.group = value("--group"),
            "--port" => {
                out.port = value("--port").parse().unwrap_or_else(|_| usage())
            }
            "--mtu" => {
                out.mtu = value("--mtu").parse().unwrap_or_else(|_| usage())
            }
            "--send" => {
                out.send_hz =
                    Some(value("--send").parse().unwrap_or_else(|_| usage()))
            }
            "--size" => {
                out.size = value("--size").parse().unwrap_or_else(|_| usage())
            }
            "--report" => {
                out.report_secs =
                    value("--report").parse().unwrap_or_else(|_| usage())
            }
            _ => usage(),
        }
    }
    if out.size < HEADER_LEN || out.size > out.mtu {
        eprintln!(
            "--size must be between {HEADER_LEN} and the mtu ({})",
            out.mtu
        );
        std::process::exit(2);
    }
    out
}

#[derive(Default)]
struct SenderStats {
    first_seq: u64,
    max_seq: u64,
    received: u64,
    out_of_order: u64,
    bytes: u64,
    window_bytes: u64,
    last_heard: Option<Instant>,
}

impl SenderStats {
    fn expected(&self) -> u64 {
        self.max_seq - self.first_seq + 1
    }

    fn loss_pct(&self) -> f64 {
        let expected = self.expected();
        if expected == 0 {
            return 0.0;
        }
        100.0 * (1.0 - (self.received.min(expected) as f64 / expected as f64))
    }
}

type Stats = Arc<Mutex<HashMap<u64, SenderStats>>>;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = parse_args();

    let medium = UdpMulticastMedium::create(&UdpMulticastConfig {
        group: args.group.clone(),
        port: args.port,
        mtu: args.mtu,
    })
    .await
    .unwrap_or_else(|err| {
        eprintln!("failed to join multicast medium: {err}");
        std::process::exit(1);
    });

    let self_id: u64 = rand::random();
    println!(
        "[bcast-probe] group {}:{} mtu {} | self id {:016x}{}",
        args.group,
        args.port,
        args.mtu,
        self_id,
        match args.send_hz {
            Some(hz) => format!(" | beaconing {hz} frames/s x {} B", args.size),
            None => " | listen only".into(),
        }
    );

    let stats: Stats = Arc::new(Mutex::new(HashMap::new()));

    // Listener.
    {
        let stats = stats.clone();
        let mut frames = medium.frames();
        tokio::spawn(async move {
            while let Some(frame) = frames.next().await {
                if frame.len() < HEADER_LEN || &frame[0..8] != MAGIC {
                    // Not a probe frame (possibly live transport traffic).
                    continue;
                }
                let sender =
                    u64::from_be_bytes(frame[8..16].try_into().unwrap());
                let seq = u64::from_be_bytes(frame[16..24].try_into().unwrap());
                let mut stats = stats.lock().unwrap();
                let entry = stats.entry(sender).or_insert_with(|| {
                    if sender != self_id {
                        println!(
                            "[bcast-probe] hearing new sender {sender:016x}"
                        );
                    }
                    SenderStats {
                        first_seq: seq,
                        max_seq: seq,
                        ..Default::default()
                    }
                });
                if seq < entry.max_seq {
                    entry.out_of_order += 1;
                }
                entry.max_seq = entry.max_seq.max(seq);
                entry.first_seq = entry.first_seq.min(seq);
                entry.received += 1;
                entry.bytes += frame.len() as u64;
                entry.window_bytes += frame.len() as u64;
                entry.last_heard = Some(Instant::now());
            }
        });
    }

    // Optional beacon.
    if let Some(hz) = args.send_hz {
        let medium = medium.clone();
        let size = args.size;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs_f64(1.0 / hz));
            let mut seq: u64 = 0;
            loop {
                interval.tick().await;
                let mut frame = Vec::with_capacity(size);
                frame.extend_from_slice(MAGIC);
                frame.extend_from_slice(&self_id.to_be_bytes());
                frame.extend_from_slice(&seq.to_be_bytes());
                frame.resize(size, 0xbc);
                if let Err(err) = medium.transmit(frame.into()).await {
                    eprintln!("transmit failed: {err}");
                }
                seq += 1;
            }
        });
    }

    // Report loop.
    let started = Instant::now();
    let mut report =
        tokio::time::interval(Duration::from_secs(args.report_secs));
    report.tick().await; // immediate first tick
    loop {
        report.tick().await;
        let now = Instant::now();
        let mut stats = stats.lock().unwrap();
        if stats.is_empty() {
            println!(
                "[{:5.0}s] no probe frames heard yet{}",
                started.elapsed().as_secs_f64(),
                if args.send_hz.is_some() {
                    " (not even our own loopback — check the transmit path)"
                } else {
                    ""
                }
            );
            continue;
        }
        let mut senders: Vec<_> = stats.iter_mut().collect();
        senders.sort_by_key(|(id, _)| **id);
        for (id, s) in senders {
            let rate = s.window_bytes as f64 / args.report_secs as f64 / 1024.0;
            s.window_bytes = 0;
            let ago = s
                .last_heard
                .map(|t| now.duration_since(t).as_secs_f64())
                .unwrap_or(f64::NAN);
            println!(
                "[{:5.0}s] {}  recv {}/{} ({:.1}% loss)  ooo {}  {:8.1} kB/s  last {:.1}s ago",
                started.elapsed().as_secs_f64(),
                if *id == self_id {
                    "self            ".to_string()
                } else {
                    format!("{id:016x}")
                },
                s.received,
                s.expected(),
                s.loss_pct(),
                s.out_of_order,
                rate,
                ago,
            );
        }
    }
}
