//! bcast-probe — an iperf-style diagnostic for the udp multicast medium.
//!
//! Answers, without involving a conductor: does multicast actually pass on
//! this LAN, and exactly where does it start failing against frame rate
//! and frame size?
//!
//! # Free-run mode (soak testing)
//!
//! ```sh
//! # machine A: beacon 10 frames/sec of 1200 bytes and listen
//! cargo run -p kitsune2_transport_broadcast --example bcast_probe -- --send 10
//!
//! # machine B: just listen
//! cargo run -p kitsune2_transport_broadcast --example bcast_probe
//! ```
//!
//! Reports per-sender received/expected counts (loss derived from sequence
//! gaps), out-of-order counts, data rate, and recency. Your own beacons
//! show up as a `self` row via multicast loopback, which confirms the
//! local transmit path independently of the network.
//!
//! # Sweep mode (envelope measurement)
//!
//! ```sh
//! # on BOTH machines:
//! cargo run -p kitsune2_transport_broadcast --example bcast_probe -- --sweep
//! ```
//!
//! Steps through a (frame rate × frame size) grid, transmitting each
//! combination for a few seconds with quiet gaps in between. Every probe
//! instance that hears sweep frames broadcasts small cumulative per-step
//! stats reports back onto the medium; the sweeping side knows exactly how
//! many frames it sent per step and prints an authoritative per-receiver
//! loss matrix at the end.
//!
//! Because a shared medium has a shared budget, concurrent sweeps would
//! contaminate each other. Sweeps therefore serialize themselves: an
//! instance defers while it hears another instance sweeping (ties broken
//! by id), so running `--sweep` on both machines measures both directions
//! back to back, and each side prints the matrix for its own transmit
//! direction.
//!
//! Probe frames use their own magic and are ignored by the broadcast
//! transport (and vice versa), so probing can safely share the group and
//! port with live nodes.

use futures::StreamExt;
use kitsune2_transport_broadcast::{UdpMulticastConfig, UdpMulticastMedium};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAGIC: &[u8; 8] = b"K2PROBE2";
const TYPE_DATA: u8 = 0;
const TYPE_REPORT: u8 = 1;

/// magic + type + sender id + step + seq.
const DATA_HEADER_LEN: usize = 8 + 1 + 8 + 2 + 8;
/// The step id used by free-run (non-sweep) beacons.
const FREE_RUN_STEP: u16 = 0xffff;

// ---------------------------------------------------------------------
// args

struct Args {
    group: String,
    port: u16,
    mtu: usize,
    send_hz: Option<f64>,
    size: usize,
    report_secs: u64,
    sweep: bool,
    rates: Vec<f64>,
    sizes: Vec<usize>,
    step_secs: f64,
}

fn usage() -> ! {
    eprintln!(
        "bcast-probe: LAN multicast diagnostic for the kitsune2 broadcast \
         transport\n\n\
         USAGE:\n  bcast_probe [OPTIONS]\n\n\
         MODES:\n\
         \x20 (default)         listen; report senders heard\n\
         \x20 --send <HZ>       also beacon numbered frames at this rate\n\
         \x20 --sweep           step a rate x size grid and print a loss\n\
         \x20                   matrix from receiver reports\n\n\
         OPTIONS:\n\
         \x20 --group <ADDR>    multicast group [default: 239.19.42.7]\n\
         \x20 --port <PORT>     shared udp port [default: 24842]\n\
         \x20 --mtu <BYTES>     max frame size [default: 1400]\n\
         \x20 --size <BYTES>    free-run beacon size [default: 1200]\n\
         \x20 --report <SECS>   free-run report interval [default: 2]\n\
         \x20 --rates <LIST>    sweep rates, frames/s\n\
         \x20                   [default: 10,25,50,100,200,400,800]\n\
         \x20 --sizes <LIST>    sweep frame sizes, bytes\n\
         \x20                   [default: 200,500,1000,1400]\n\
         \x20 --step-secs <S>   transmit time per sweep step [default: 3]\n\
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
        sweep: false,
        rates: vec![10.0, 25.0, 50.0, 100.0, 200.0, 400.0, 800.0],
        sizes: vec![200, 500, 1000, 1400],
        step_secs: 3.0,
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
            "--sweep" => out.sweep = true,
            "--rates" => {
                out.rates = value("--rates")
                    .split(',')
                    .map(|v| v.trim().parse().unwrap_or_else(|_| usage()))
                    .collect()
            }
            "--sizes" => {
                out.sizes = value("--sizes")
                    .split(',')
                    .map(|v| v.trim().parse().unwrap_or_else(|_| usage()))
                    .collect()
            }
            "--step-secs" => {
                out.step_secs =
                    value("--step-secs").parse().unwrap_or_else(|_| usage())
            }
            _ => usage(),
        }
    }
    if out.sweep && out.send_hz.is_some() {
        eprintln!("--sweep and --send are mutually exclusive");
        std::process::exit(2);
    }
    for &size in out.sizes.iter().chain(std::iter::once(&out.size)) {
        if size < DATA_HEADER_LEN || size > out.mtu {
            eprintln!(
                "frame size {size} must be between {DATA_HEADER_LEN} and \
                 the mtu ({})",
                out.mtu
            );
            std::process::exit(2);
        }
    }
    if out.sweep && out.rates.len() * out.sizes.len() > FREE_RUN_STEP as usize {
        eprintln!("sweep grid too large");
        std::process::exit(2);
    }
    out
}

// ---------------------------------------------------------------------
// wire format

fn encode_data(sender: u64, step: u16, seq: u64, size: usize) -> Vec<u8> {
    let mut frame = Vec::with_capacity(size);
    frame.extend_from_slice(MAGIC);
    frame.push(TYPE_DATA);
    frame.extend_from_slice(&sender.to_be_bytes());
    frame.extend_from_slice(&step.to_be_bytes());
    frame.extend_from_slice(&seq.to_be_bytes());
    frame.resize(size, 0xbc);
    frame
}

struct DataFrame {
    sender: u64,
    step: u16,
    seq: u64,
}

/// One receiver's cumulative counters for one (subject, step).
#[derive(Debug, Default, Clone, Copy)]
struct StepCount {
    received: u64,
    bytes: u64,
}

/// REPORT: magic + type + observer + subject + count + count x
/// (step u16, received u64, bytes u64).
fn encode_report(
    observer: u64,
    subject: u64,
    steps: &[(u16, StepCount)],
    mtu: usize,
) -> Vec<u8> {
    let per_entry = 2 + 8 + 8;
    let max_entries = (mtu - (8 + 1 + 8 + 8 + 1)) / per_entry;
    let steps = &steps[..steps.len().min(max_entries).min(u8::MAX as usize)];
    let mut frame = Vec::with_capacity(mtu);
    frame.extend_from_slice(MAGIC);
    frame.push(TYPE_REPORT);
    frame.extend_from_slice(&observer.to_be_bytes());
    frame.extend_from_slice(&subject.to_be_bytes());
    frame.push(steps.len() as u8);
    for (step, count) in steps {
        frame.extend_from_slice(&step.to_be_bytes());
        frame.extend_from_slice(&count.received.to_be_bytes());
        frame.extend_from_slice(&count.bytes.to_be_bytes());
    }
    frame
}

enum Frame {
    Data(DataFrame),
    Report {
        observer: u64,
        subject: u64,
        steps: Vec<(u16, StepCount)>,
    },
}

fn decode(frame: &[u8]) -> Option<Frame> {
    if frame.len() < 9 || &frame[0..8] != MAGIC {
        return None;
    }
    match frame[8] {
        TYPE_DATA => {
            if frame.len() < DATA_HEADER_LEN {
                return None;
            }
            Some(Frame::Data(DataFrame {
                sender: u64::from_be_bytes(frame[9..17].try_into().ok()?),
                step: u16::from_be_bytes(frame[17..19].try_into().ok()?),
                seq: u64::from_be_bytes(frame[19..27].try_into().ok()?),
            }))
        }
        TYPE_REPORT => {
            if frame.len() < 26 {
                return None;
            }
            let observer = u64::from_be_bytes(frame[9..17].try_into().ok()?);
            let subject = u64::from_be_bytes(frame[17..25].try_into().ok()?);
            let count = frame[25] as usize;
            let mut steps = Vec::with_capacity(count);
            let mut at = 26;
            for _ in 0..count {
                if frame.len() < at + 18 {
                    return None;
                }
                steps.push((
                    u16::from_be_bytes(frame[at..at + 2].try_into().ok()?),
                    StepCount {
                        received: u64::from_be_bytes(
                            frame[at + 2..at + 10].try_into().ok()?,
                        ),
                        bytes: u64::from_be_bytes(
                            frame[at + 10..at + 18].try_into().ok()?,
                        ),
                    },
                ));
                at += 18;
            }
            Some(Frame::Report {
                observer,
                subject,
                steps,
            })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------
// shared state

/// Free-run stats for one sender (seq-gap based).
#[derive(Default)]
struct FreeRunStats {
    first_seq: u64,
    max_seq: u64,
    received: u64,
    out_of_order: u64,
    window_bytes: u64,
    last_heard: Option<Instant>,
}

impl FreeRunStats {
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

#[derive(Default)]
struct Shared {
    /// Free-run rows keyed by sender.
    free_run: HashMap<u64, FreeRunStats>,
    /// What we, as an observer, have counted: (subject, step) -> counts.
    observed: HashMap<u64, HashMap<u16, StepCount>>,
    /// Last time we heard sweep DATA per subject (drives report tail +
    /// polite deferral).
    sweep_heard: HashMap<u64, Instant>,
    /// Reports about OUR transmissions: observer -> step -> counts.
    reports: HashMap<u64, HashMap<u16, StepCount>>,
}

impl Shared {
    fn hears_other_sweep(&self, self_id: u64, within: Duration) -> Option<u64> {
        let now = Instant::now();
        self.sweep_heard
            .iter()
            .filter(|(id, at)| {
                **id != self_id && now.duration_since(**at) < within
            })
            .map(|(id, _)| *id)
            .next()
    }
}

type SharedState = Arc<Mutex<Shared>>;

// ---------------------------------------------------------------------
// main

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
        "[bcast-probe] group {}:{} mtu {} | self id {:016x} | {}",
        args.group,
        args.port,
        args.mtu,
        self_id,
        if args.sweep {
            format!(
                "sweep {} rates x {} sizes, {}s/step",
                args.rates.len(),
                args.sizes.len(),
                args.step_secs
            )
        } else {
            match args.send_hz {
                Some(hz) => {
                    format!("beaconing {hz} frames/s x {} B", args.size)
                }
                None => "listen only".into(),
            }
        }
    );

    let shared: SharedState = Arc::new(Mutex::new(Shared::default()));

    // Listener: classifies every heard probe frame.
    {
        let shared = shared.clone();
        let mut frames = medium.frames();
        tokio::spawn(async move {
            while let Some(frame) = frames.next().await {
                let frame_len = frame.len();
                match decode(&frame) {
                    Some(Frame::Data(data)) => {
                        let mut shared = shared.lock().unwrap();
                        if data.step == FREE_RUN_STEP {
                            let entry = shared
                                .free_run
                                .entry(data.sender)
                                .or_insert_with(|| {
                                    if data.sender != self_id {
                                        println!(
                                            "[bcast-probe] hearing new \
                                             sender {:016x}",
                                            data.sender
                                        );
                                    }
                                    FreeRunStats {
                                        first_seq: data.seq,
                                        max_seq: data.seq,
                                        ..Default::default()
                                    }
                                });
                            if data.seq < entry.max_seq {
                                entry.out_of_order += 1;
                            }
                            entry.max_seq = entry.max_seq.max(data.seq);
                            entry.first_seq = entry.first_seq.min(data.seq);
                            entry.received += 1;
                            entry.window_bytes += frame_len as u64;
                            entry.last_heard = Some(Instant::now());
                        } else {
                            // Sweep data: count it (own frames excluded —
                            // loopback would report perfect scores).
                            if data.sender != self_id {
                                if !shared.observed.contains_key(&data.sender) {
                                    println!(
                                        "[bcast-probe] observing sweep \
                                         from {:016x}, reporting stats \
                                         back",
                                        data.sender
                                    );
                                }
                                let entry = shared
                                    .observed
                                    .entry(data.sender)
                                    .or_default()
                                    .entry(data.step)
                                    .or_default();
                                entry.received += 1;
                                entry.bytes += frame_len as u64;
                                shared
                                    .sweep_heard
                                    .insert(data.sender, Instant::now());
                            }
                        }
                    }
                    // Cumulative: latest report wins.
                    Some(Frame::Report {
                        observer,
                        subject,
                        steps,
                    }) if subject == self_id && observer != self_id => {
                        let mut shared = shared.lock().unwrap();
                        let per_observer =
                            shared.reports.entry(observer).or_default();
                        for (step, count) in steps {
                            let entry = per_observer.entry(step).or_default();
                            if count.received > entry.received {
                                *entry = count;
                            }
                        }
                    }
                    Some(Frame::Report { .. }) | None => {}
                }
            }
        });
    }

    // Reporter: while hearing anyone's sweep (and a tail after), broadcast
    // cumulative per-step counts back to them. Tiny frames; cumulative, so
    // losing some is harmless.
    {
        let shared = shared.clone();
        let medium = medium.clone();
        let mtu = args.mtu;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            loop {
                tick.tick().await;
                let outgoing: Vec<Vec<u8>> = {
                    let shared = shared.lock().unwrap();
                    let now = Instant::now();
                    shared
                        .observed
                        .iter()
                        .filter(|(subject, _)| {
                            shared.sweep_heard.get(*subject).is_some_and(|at| {
                                now.duration_since(*at) < Duration::from_secs(5)
                            })
                        })
                        .map(|(subject, steps)| {
                            let mut steps: Vec<(u16, StepCount)> = steps
                                .iter()
                                .map(|(step, count)| (*step, *count))
                                .collect();
                            steps.sort_by_key(|(step, _)| *step);
                            encode_report(self_id, *subject, &steps, mtu)
                        })
                        .collect()
                };
                for frame in outgoing {
                    let _ = medium.transmit(frame.into()).await;
                }
            }
        });
    }

    if args.sweep {
        run_sweep(&args, self_id, &medium, &shared).await;
        return;
    }

    // Free-run beacon.
    if let Some(hz) = args.send_hz {
        let medium = medium.clone();
        let size = args.size;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs_f64(1.0 / hz));
            let mut seq: u64 = 0;
            loop {
                interval.tick().await;
                let frame = encode_data(self_id, FREE_RUN_STEP, seq, size);
                if let Err(err) = medium.transmit(frame.into()).await {
                    eprintln!("transmit failed: {err}");
                }
                seq += 1;
            }
        });
    }

    // Free-run report loop.
    let started = Instant::now();
    let mut report =
        tokio::time::interval(Duration::from_secs(args.report_secs));
    report.tick().await;
    loop {
        report.tick().await;
        let now = Instant::now();
        let mut shared = shared.lock().unwrap();
        if shared.free_run.is_empty() {
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
        let mut senders: Vec<_> = shared.free_run.iter_mut().collect();
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

// ---------------------------------------------------------------------
// sweep

async fn run_sweep(
    args: &Args,
    self_id: u64,
    medium: &kitsune2_transport_broadcast::medium::DynBroadcastMedium,
    shared: &SharedState,
) {
    // Polite deferral: never sweep while someone else is sweeping. Ties
    // (both waiting) break toward the lower id, which starts first.
    let quiet = Duration::from_millis(1500);
    loop {
        let listen_for = Duration::from_millis(2000 + self_id % 2000);
        tokio::time::sleep(listen_for).await;
        let other = shared.lock().unwrap().hears_other_sweep(self_id, quiet);
        match other {
            Some(other) => {
                println!("[bcast-probe] deferring: {other:016x} is sweeping");
                // Wait until they've been quiet for a while.
                loop {
                    tokio::time::sleep(quiet).await;
                    if shared
                        .lock()
                        .unwrap()
                        .hears_other_sweep(self_id, quiet)
                        .is_none()
                    {
                        break;
                    }
                }
                // Loop back to the listen window before starting.
            }
            None => break,
        }
    }

    // The grid. step id = row-major index.
    let steps: Vec<(f64, usize)> = args
        .sizes
        .iter()
        .flat_map(|size| args.rates.iter().map(|rate| (*rate, *size)))
        .collect();
    let mut sent_per_step: Vec<u64> = vec![0; steps.len()];

    println!(
        "[bcast-probe] sweeping {} steps ({} rates x {} sizes), {}s each",
        steps.len(),
        args.rates.len(),
        args.sizes.len(),
        args.step_secs
    );

    for (index, (rate, size)) in steps.iter().enumerate() {
        println!(
            "[bcast-probe] step {}/{}: {} frames/s x {} B ({:.1} kB/s offered)",
            index + 1,
            steps.len(),
            rate,
            size,
            rate * *size as f64 / 1024.0
        );
        let mut interval =
            tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
        interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
        let step_end = Instant::now() + Duration::from_secs_f64(args.step_secs);
        let mut seq: u64 = 0;
        while Instant::now() < step_end {
            interval.tick().await;
            let frame = encode_data(self_id, index as u16, seq, *size);
            if let Err(err) = medium.transmit(frame.into()).await {
                eprintln!("transmit failed: {err}");
            }
            seq += 1;
        }
        sent_per_step[index] = seq;
        // Quiet gap: drain queues/buffers so steps don't bleed together.
        tokio::time::sleep(Duration::from_millis(700)).await;
    }

    // Tail: let the last cumulative reports arrive.
    println!("[bcast-probe] sweep done; collecting final reports...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let reports = shared.lock().unwrap().reports.clone();
    if reports.is_empty() {
        println!(
            "[bcast-probe] no receiver reports heard — is another probe \
             instance running on the far side?"
        );
        return;
    }

    for (observer, per_step) in {
        let mut observers: Vec<_> = reports.into_iter().collect();
        observers.sort_by_key(|(id, _)| *id);
        observers
    } {
        println!(
            "\nloss % of our frames as received by {observer:016x} \
             (rows: frame size B, cols: frames/s):"
        );
        print!("{:>8}", "");
        for rate in &args.rates {
            print!("{rate:>9}");
        }
        println!();
        for (size_index, size) in args.sizes.iter().enumerate() {
            print!("{size:>8}");
            for rate_index in 0..args.rates.len() {
                let step = size_index * args.rates.len() + rate_index;
                let sent = sent_per_step[step];
                match per_step.get(&(step as u16)) {
                    Some(count) if sent > 0 => {
                        let loss = 100.0
                            * (1.0
                                - (count.received.min(sent) as f64
                                    / sent as f64));
                        print!("{loss:>8.1}%");
                    }
                    _ => print!("{:>9}", "-"),
                }
            }
            println!();
        }
    }
    println!(
        "\n('-' = no frames of that step were reported received; offered \
         kB/s = rate x size / 1024)"
    );
}
