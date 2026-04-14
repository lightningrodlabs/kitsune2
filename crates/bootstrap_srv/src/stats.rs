//! Lightweight historical metrics collection using multi-resolution ring buffers.
//!
//! Memory footprint: ~2.8 KB total across all resolution levels.

use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A fixed-capacity ring buffer.
struct RingBuffer<T> {
    buf: Vec<T>,
    /// Index of the next write position.
    head: usize,
    /// Number of elements currently stored.
    len: usize,
}

impl<T: Copy + Default> RingBuffer<T> {
    fn new(capacity: usize) -> Self {
        Self {
            buf: vec![T::default(); capacity],
            head: 0,
            len: 0,
        }
    }

    fn push(&mut self, item: T) {
        self.buf[self.head] = item;
        self.head = (self.head + 1) % self.buf.len();
        if self.len < self.buf.len() {
            self.len += 1;
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    /// Iterate over all stored elements from oldest to newest.
    fn iter(&self) -> impl Iterator<Item = &T> {
        let cap = self.buf.len();
        let start = if self.len < cap {
            0
        } else {
            self.head
        };
        (0..self.len).map(move |i| &self.buf[(start + i) % cap])
    }

    /// Iterate over the last `n` elements (newest).
    fn iter_last(&self, n: usize) -> impl Iterator<Item = &T> {
        let n = n.min(self.len);
        let cap = self.buf.len();
        // The newest element is at (head - 1), so the last n start at (head - n).
        let start = if self.len < cap {
            self.len - n
        } else {
            (self.head + cap - n) % cap
        };
        (0..n).map(move |i| &self.buf[(start + i) % cap])
    }
}

/// A single point-in-time snapshot of server stats.
#[derive(Clone, Copy, Default)]
struct Snapshot {
    total_spaces: u32,
    total_agents: u32,
    min_agents_per_space: u32,
    max_agents_per_space: u32,
}

/// Tracks min, max, and sum for computing aggregates.
#[derive(Clone, Copy)]
struct MinMaxSum {
    min: u32,
    max: u32,
    sum: u64,
}

impl Default for MinMaxSum {
    fn default() -> Self {
        Self {
            min: u32::MAX,
            max: 0,
            sum: 0,
        }
    }
}

impl MinMaxSum {
    fn record(&mut self, value: u32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value as u64;
    }

    fn to_json(&self, count: u32) -> serde_json::Value {
        let avg = if count > 0 {
            self.sum as f64 / count as f64
        } else {
            0.0
        };
        serde_json::json!({
            "min": self.min,
            "max": self.max,
            "avg": avg,
        })
    }
}

/// An aggregate over multiple snapshots for a time window.
#[derive(Clone, Copy, Default)]
struct WindowAggregate {
    sample_count: u32,
    total_spaces: MinMaxSum,
    total_agents: MinMaxSum,
    min_agents_per_space: MinMaxSum,
    max_agents_per_space: MinMaxSum,
}

impl WindowAggregate {
    fn record(&mut self, snap: &Snapshot) {
        self.sample_count += 1;
        self.total_spaces.record(snap.total_spaces);
        self.total_agents.record(snap.total_agents);
        self.min_agents_per_space.record(snap.min_agents_per_space);
        self.max_agents_per_space.record(snap.max_agents_per_space);
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "samples": self.sample_count,
            "totalSpaces": self.total_spaces.to_json(self.sample_count),
            "totalAgents": self.total_agents.to_json(self.sample_count),
            "minAgentsPerSpace": self.min_agents_per_space.to_json(self.sample_count),
            "maxAgentsPerSpace": self.max_agents_per_space.to_json(self.sample_count),
        })
    }
}

/// Aggregate an iterator of snapshots into a WindowAggregate.
fn aggregate_snapshots<'a>(
    iter: impl Iterator<Item = &'a Snapshot>,
) -> WindowAggregate {
    let mut agg = WindowAggregate::default();
    for snap in iter {
        agg.record(snap);
    }
    agg
}

/// Aggregate an iterator of WindowAggregates into a single WindowAggregate.
fn aggregate_windows<'a>(
    iter: impl Iterator<Item = &'a WindowAggregate>,
) -> WindowAggregate {
    let mut out = WindowAggregate::default();
    for w in iter {
        if w.sample_count == 0 {
            continue;
        }
        out.sample_count += w.sample_count;

        out.total_spaces.min = out.total_spaces.min.min(w.total_spaces.min);
        out.total_spaces.max = out.total_spaces.max.max(w.total_spaces.max);
        out.total_spaces.sum += w.total_spaces.sum;

        out.total_agents.min = out.total_agents.min.min(w.total_agents.min);
        out.total_agents.max = out.total_agents.max.max(w.total_agents.max);
        out.total_agents.sum += w.total_agents.sum;

        out.min_agents_per_space.min =
            out.min_agents_per_space.min.min(w.min_agents_per_space.min);
        out.min_agents_per_space.max =
            out.min_agents_per_space.max.max(w.min_agents_per_space.max);
        out.min_agents_per_space.sum += w.min_agents_per_space.sum;

        out.max_agents_per_space.min =
            out.max_agents_per_space.min.min(w.max_agents_per_space.min);
        out.max_agents_per_space.max =
            out.max_agents_per_space.max.max(w.max_agents_per_space.max);
        out.max_agents_per_space.sum += w.max_agents_per_space.sum;
    }
    out
}

struct MetricsInner {
    start_time: Instant,
    /// Level 0: per-snapshot, capacity 60 (last hour at ~1 min intervals).
    minutes: RingBuffer<Snapshot>,
    /// Level 1: hourly aggregates, capacity 24 (last day).
    hours: RingBuffer<WindowAggregate>,
    /// Level 2: daily aggregates, capacity 31 (last month; last 7 = last week).
    days: RingBuffer<WindowAggregate>,
    /// Level 3: all-time running aggregate.
    all_time: WindowAggregate,
    /// Monotonic counter of snapshots recorded since last hourly cascade.
    snapshots_since_hour_cascade: u32,
    /// Monotonic counter of hourly aggregates since last daily cascade.
    hours_since_day_cascade: u32,
}

/// Thread-safe metrics collector for historical server stats.
///
/// Cheaply cloneable (inner state is behind `Arc<Mutex>`).
/// Memory footprint: ~2.8 KB across all resolution levels.
#[derive(Clone)]
pub struct MetricsCollector(Arc<Mutex<MetricsInner>>);

impl MetricsCollector {
    /// How many snapshots per hour cascade.
    const SNAPSHOTS_PER_HOUR: u32 = 60;
    /// How many hour cascades per day cascade.
    const HOURS_PER_DAY: u32 = 24;

    /// Create a new metrics collector with empty ring buffers.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MetricsInner {
            start_time: Instant::now(),
            minutes: RingBuffer::new(60),
            hours: RingBuffer::new(24),
            days: RingBuffer::new(31),
            all_time: WindowAggregate::default(),
            snapshots_since_hour_cascade: 0,
            hours_since_day_cascade: 0,
        })))
    }

    /// Record a new snapshot from the current space map stats.
    ///
    /// Call this once per prune interval (~60s in production).
    pub fn record_snapshot(
        &self,
        total_spaces: usize,
        total_agents: usize,
        min_agents: usize,
        max_agents: usize,
    ) {
        let snap = Snapshot {
            total_spaces: total_spaces as u32,
            total_agents: total_agents as u32,
            min_agents_per_space: min_agents as u32,
            max_agents_per_space: max_agents as u32,
        };

        let mut inner = self.0.lock().unwrap();

        // Push to level 0 (minute ring buffer).
        inner.minutes.push(snap);

        // Update all-time aggregate.
        inner.all_time.record(&snap);

        // Cascade: minute -> hour.
        inner.snapshots_since_hour_cascade += 1;
        if inner.snapshots_since_hour_cascade >= Self::SNAPSHOTS_PER_HOUR {
            let hour_agg = aggregate_snapshots(inner.minutes.iter());
            inner.hours.push(hour_agg);
            inner.snapshots_since_hour_cascade = 0;

            // Cascade: hour -> day.
            inner.hours_since_day_cascade += 1;
            if inner.hours_since_day_cascade >= Self::HOURS_PER_DAY {
                let day_agg = aggregate_windows(inner.hours.iter());
                inner.days.push(day_agg);
                inner.hours_since_day_cascade = 0;
            }
        }
    }

    /// Build the full JSON response for the `/metrics` endpoint.
    ///
    /// `current_stats` is `(num_spaces, total_agents, min_agents, max_agents)`
    /// from `SpaceMap::stats()`, computed on-demand at request time.
    pub fn to_json(
        &self,
        current_stats: (usize, usize, usize, usize),
    ) -> serde_json::Value {
        let (num_spaces, total_agents, min_agents, max_agents) = current_stats;
        let avg_agents = if num_spaces > 0 {
            total_agents as f64 / num_spaces as f64
        } else {
            0.0
        };

        let inner = self.0.lock().unwrap();
        let uptime_secs = inner.start_time.elapsed().as_secs();

        let mut resp = serde_json::json!({
            "uptimeSecs": uptime_secs,
            "snapshotIntervalSecs": Self::SNAPSHOTS_PER_HOUR,
            "current": {
                "totalSpaces": num_spaces,
                "totalAgents": total_agents,
                "avgAgentsPerSpace": avg_agents,
                "minAgentsPerSpace": min_agents,
                "maxAgentsPerSpace": max_agents,
            },
        });

        // Last hour: aggregate level 0 snapshots.
        if inner.minutes.len() > 0 {
            let agg = aggregate_snapshots(inner.minutes.iter());
            resp["lastHour"] = agg.to_json();
        }

        // Last day: aggregate level 1 hourly entries.
        if inner.hours.len() > 0 {
            let agg = aggregate_windows(inner.hours.iter());
            resp["lastDay"] = agg.to_json();
        }

        // Last week: aggregate last 7 daily entries from level 2.
        if inner.days.len() > 0 {
            let week_agg = aggregate_windows(inner.days.iter_last(7));
            resp["lastWeek"] = week_agg.to_json();

            // Last month: aggregate all level 2 daily entries.
            let month_agg = aggregate_windows(inner.days.iter());
            resp["lastMonth"] = month_agg.to_json();
        }

        // All-time.
        if inner.all_time.sample_count > 0 {
            resp["allTime"] = inner.all_time.to_json();
        }

        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_basic() {
        let mut rb: RingBuffer<u32> = RingBuffer::new(3);
        assert_eq!(rb.len(), 0);

        rb.push(1);
        rb.push(2);
        assert_eq!(rb.len(), 2);
        assert_eq!(
            rb.iter().copied().collect::<Vec<_>>(),
            vec![1, 2],
        );

        rb.push(3);
        rb.push(4); // overwrites 1
        assert_eq!(rb.len(), 3);
        assert_eq!(
            rb.iter().copied().collect::<Vec<_>>(),
            vec![2, 3, 4],
        );
    }

    #[test]
    fn ring_buffer_iter_last() {
        let mut rb: RingBuffer<u32> = RingBuffer::new(5);
        for i in 1..=7 {
            rb.push(i);
        }
        // Buffer contains [3, 4, 5, 6, 7]
        assert_eq!(
            rb.iter_last(3).copied().collect::<Vec<_>>(),
            vec![5, 6, 7],
        );
        assert_eq!(
            rb.iter_last(10).copied().collect::<Vec<_>>(),
            vec![3, 4, 5, 6, 7],
        );
    }

    #[test]
    fn metrics_collector_snapshot_and_json() {
        let mc = MetricsCollector::new();

        // Record a few snapshots.
        mc.record_snapshot(2, 10, 3, 7);
        mc.record_snapshot(3, 15, 2, 8);
        mc.record_snapshot(2, 12, 4, 6);

        let json = mc.to_json((2, 12, 4, 6));
        let obj = json.as_object().unwrap();

        // Should have current and lastHour, allTime.
        assert!(obj.contains_key("current"));
        assert!(obj.contains_key("lastHour"));
        assert!(obj.contains_key("allTime"));

        // No hourly/daily cascades yet.
        assert!(!obj.contains_key("lastDay"));
        assert!(!obj.contains_key("lastWeek"));
        assert!(!obj.contains_key("lastMonth"));

        // Verify current values.
        assert_eq!(json["current"]["totalSpaces"], 2);
        assert_eq!(json["current"]["totalAgents"], 12);

        // Verify lastHour aggregates.
        assert_eq!(json["lastHour"]["samples"], 3);
        assert_eq!(json["lastHour"]["totalSpaces"]["min"], 2);
        assert_eq!(json["lastHour"]["totalSpaces"]["max"], 3);
        assert_eq!(json["lastHour"]["totalAgents"]["min"], 10);
        assert_eq!(json["lastHour"]["totalAgents"]["max"], 15);

        // Verify allTime matches lastHour (same data so far).
        assert_eq!(json["allTime"]["samples"], 3);
        assert_eq!(json["allTime"]["totalAgents"]["min"], 10);
        assert_eq!(json["allTime"]["totalAgents"]["max"], 15);
    }

    #[test]
    fn metrics_collector_hour_cascade() {
        let mc = MetricsCollector::new();

        // Record 60 snapshots to trigger an hourly cascade.
        for i in 0..60 {
            mc.record_snapshot(1, i + 1, 1, i + 1);
        }

        let json = mc.to_json((1, 60, 1, 60));
        let obj = json.as_object().unwrap();

        // Should now have lastDay (hourly cascade happened).
        assert!(obj.contains_key("lastDay"));
        assert_eq!(json["lastDay"]["samples"], 60);
        assert_eq!(json["lastDay"]["totalAgents"]["min"], 1);
        assert_eq!(json["lastDay"]["totalAgents"]["max"], 60);
    }

    #[test]
    fn metrics_collector_day_cascade() {
        let mc = MetricsCollector::new();

        // 60 snapshots * 24 hours = 1440 snapshots to trigger a daily cascade.
        for i in 0..1440 {
            mc.record_snapshot(1, i + 1, 1, i + 1);
        }

        let json = mc.to_json((1, 1440, 1, 1440));
        let obj = json.as_object().unwrap();

        assert!(obj.contains_key("lastWeek"));
        assert!(obj.contains_key("lastMonth"));
    }
}
