//! Lightweight historical metrics collection using multi-resolution ring buffers.
//!
//! Memory footprint: ~6 KB total across all resolution levels.

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
    total_cells: u32,
    unique_agents: u32,
    min_cells_per_space: u32,
    max_cells_per_space: u32,
    groups: u32,
    tool_instances: u32,
    group_member_sum: u32,
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
    total_cells: MinMaxSum,
    unique_agents: MinMaxSum,
    min_cells_per_space: MinMaxSum,
    max_cells_per_space: MinMaxSum,
    groups: MinMaxSum,
    tool_instances: MinMaxSum,
    group_member_sum: MinMaxSum,
}

impl WindowAggregate {
    fn record(&mut self, snap: &Snapshot) {
        self.sample_count += 1;
        self.total_spaces.record(snap.total_spaces);
        self.total_cells.record(snap.total_cells);
        self.unique_agents.record(snap.unique_agents);
        self.min_cells_per_space.record(snap.min_cells_per_space);
        self.max_cells_per_space.record(snap.max_cells_per_space);
        self.groups.record(snap.groups);
        self.tool_instances.record(snap.tool_instances);
        self.group_member_sum.record(snap.group_member_sum);
    }

    fn to_json(&self) -> serde_json::Value {
        // Sample-weighted average of cells-per-agent over the window:
        // total cells seen across samples over total unique agents seen.
        let avg_cells_per_agent = if self.unique_agents.sum > 0 {
            self.total_cells.sum as f64 / self.unique_agents.sum as f64
        } else {
            0.0
        };
        // Sample-weighted ratios over the window.
        let avg_people_per_group = if self.groups.sum > 0 {
            self.group_member_sum.sum as f64 / self.groups.sum as f64
        } else {
            0.0
        };
        let avg_tools_per_group = if self.groups.sum > 0 {
            self.tool_instances.sum as f64 / self.groups.sum as f64
        } else {
            0.0
        };
        serde_json::json!({
            "samples": self.sample_count,
            "totalSpaces": self.total_spaces.to_json(self.sample_count),
            "totalCells": self.total_cells.to_json(self.sample_count),
            "uniqueAgents": self.unique_agents.to_json(self.sample_count),
            "avgCellsPerAgent": avg_cells_per_agent,
            "minCellsPerSpace": self.min_cells_per_space.to_json(self.sample_count),
            "maxCellsPerSpace": self.max_cells_per_space.to_json(self.sample_count),
            "groups": self.groups.to_json(self.sample_count),
            "toolInstances": self.tool_instances.to_json(self.sample_count),
            "avgPeoplePerGroup": avg_people_per_group,
            "avgToolsPerGroup": avg_tools_per_group,
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

        out.total_cells.min = out.total_cells.min.min(w.total_cells.min);
        out.total_cells.max = out.total_cells.max.max(w.total_cells.max);
        out.total_cells.sum += w.total_cells.sum;

        out.unique_agents.min = out.unique_agents.min.min(w.unique_agents.min);
        out.unique_agents.max = out.unique_agents.max.max(w.unique_agents.max);
        out.unique_agents.sum += w.unique_agents.sum;

        out.min_cells_per_space.min =
            out.min_cells_per_space.min.min(w.min_cells_per_space.min);
        out.min_cells_per_space.max =
            out.min_cells_per_space.max.max(w.min_cells_per_space.max);
        out.min_cells_per_space.sum += w.min_cells_per_space.sum;

        out.max_cells_per_space.min =
            out.max_cells_per_space.min.min(w.max_cells_per_space.min);
        out.max_cells_per_space.max =
            out.max_cells_per_space.max.max(w.max_cells_per_space.max);
        out.max_cells_per_space.sum += w.max_cells_per_space.sum;

        out.groups.min = out.groups.min.min(w.groups.min);
        out.groups.max = out.groups.max.max(w.groups.max);
        out.groups.sum += w.groups.sum;

        out.tool_instances.min =
            out.tool_instances.min.min(w.tool_instances.min);
        out.tool_instances.max =
            out.tool_instances.max.max(w.tool_instances.max);
        out.tool_instances.sum += w.tool_instances.sum;

        out.group_member_sum.min =
            out.group_member_sum.min.min(w.group_member_sum.min);
        out.group_member_sum.max =
            out.group_member_sum.max.max(w.group_member_sum.max);
        out.group_member_sum.sum += w.group_member_sum.sum;
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
/// Memory footprint: ~6 KB across all resolution levels.
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
    pub fn record_snapshot(&self, stats: crate::SpaceStats) {
        let snap = Snapshot {
            total_spaces: stats.spaces as u32,
            total_cells: stats.cells as u32,
            unique_agents: stats.unique_agents as u32,
            min_cells_per_space: stats.min_cells_per_space as u32,
            max_cells_per_space: stats.max_cells_per_space as u32,
            groups: stats.groups as u32,
            tool_instances: stats.tool_instances as u32,
            group_member_sum: stats.group_member_sum as u32,
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
    /// `current_stats` is a [`crate::SpaceStats`] from `SpaceMap::stats()`,
    /// computed on-demand at request time.
    pub fn to_json(
        &self,
        current_stats: crate::SpaceStats,
    ) -> serde_json::Value {
        let avg_cells_per_space = if current_stats.spaces > 0 {
            current_stats.cells as f64 / current_stats.spaces as f64
        } else {
            0.0
        };
        let avg_cells_per_agent = if current_stats.unique_agents > 0 {
            current_stats.cells as f64 / current_stats.unique_agents as f64
        } else {
            0.0
        };
        let avg_people_per_group = if current_stats.groups > 0 {
            current_stats.group_member_sum as f64 / current_stats.groups as f64
        } else {
            0.0
        };
        let avg_tools_per_group = if current_stats.groups > 0 {
            current_stats.tool_instances as f64 / current_stats.groups as f64
        } else {
            0.0
        };

        let inner = self.0.lock().unwrap();
        let uptime_secs = inner.start_time.elapsed().as_secs();

        let mut resp = serde_json::json!({
            "uptimeSecs": uptime_secs,
            "snapshotIntervalSecs": Self::SNAPSHOTS_PER_HOUR,
            "current": {
                "totalSpaces": current_stats.spaces,
                "totalCells": current_stats.cells,
                "uniqueAgents": current_stats.unique_agents,
                "avgCellsPerAgent": avg_cells_per_agent,
                "avgCellsPerSpace": avg_cells_per_space,
                "minCellsPerSpace": current_stats.min_cells_per_space,
                "maxCellsPerSpace": current_stats.max_cells_per_space,
                "groups": current_stats.groups,
                "toolInstances": current_stats.tool_instances,
                "avgPeoplePerGroup": avg_people_per_group,
                "avgToolsPerGroup": avg_tools_per_group,
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

        // Raw history series (oldest to newest) so consumers can plot
        // trends: per-minute snapshots, then hourly and daily averages.
        let minutes: Vec<serde_json::Value> = inner
            .minutes
            .iter()
            .map(|s| {
                let r = |num: u32, den: u32| {
                    if den > 0 {
                        (num as f64 / den as f64 * 10.0).round() / 10.0
                    } else {
                        0.0
                    }
                };
                serde_json::json!({
                    "spaces": s.total_spaces,
                    "cells": s.total_cells,
                    "uniqueAgents": s.unique_agents,
                    "groups": s.groups,
                    "tools": s.tool_instances,
                    "peoplePerGroup": r(s.group_member_sum, s.groups),
                    "toolsPerGroup": r(s.tool_instances, s.groups),
                })
            })
            .collect();
        let window_point = |w: &WindowAggregate| {
            let avg = |m: &MinMaxSum| {
                if w.sample_count > 0 {
                    (m.sum as f64 / w.sample_count as f64 * 10.0).round()
                        / 10.0
                } else {
                    0.0
                }
            };
            let ratio = |num: &MinMaxSum, den: &MinMaxSum| {
                if den.sum > 0 {
                    (num.sum as f64 / den.sum as f64 * 10.0).round() / 10.0
                } else {
                    0.0
                }
            };
            serde_json::json!({
                "spaces": avg(&w.total_spaces),
                "cells": avg(&w.total_cells),
                "uniqueAgents": avg(&w.unique_agents),
                "groups": avg(&w.groups),
                "tools": avg(&w.tool_instances),
                "peoplePerGroup": ratio(&w.group_member_sum, &w.groups),
                "toolsPerGroup": ratio(&w.tool_instances, &w.groups),
            })
        };
        let hours: Vec<serde_json::Value> =
            inner.hours.iter().map(window_point).collect();
        let days: Vec<serde_json::Value> =
            inner.days.iter().map(window_point).collect();
        resp["series"] = serde_json::json!({
            "minutes": minutes,
            "hours": hours,
            "days": days,
        });

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

    fn stats(
        spaces: usize,
        cells: usize,
        unique_agents: usize,
        min_cells_per_space: usize,
        max_cells_per_space: usize,
    ) -> crate::SpaceStats {
        crate::SpaceStats {
            spaces,
            cells,
            unique_agents,
            min_cells_per_space,
            max_cells_per_space,
            ..Default::default()
        }
    }

    #[test]
    fn metrics_collector_snapshot_and_json() {
        let mc = MetricsCollector::new();

        // Record a few snapshots.
        mc.record_snapshot(stats(2, 10, 5, 3, 7));
        mc.record_snapshot(stats(3, 15, 6, 2, 8));
        mc.record_snapshot(stats(2, 12, 4, 4, 6));

        let json = mc.to_json(stats(2, 12, 4, 4, 6));
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
        assert_eq!(json["current"]["totalCells"], 12);
        assert_eq!(json["current"]["uniqueAgents"], 4);
        assert_eq!(json["current"]["avgCellsPerAgent"], 3.0);

        // Verify lastHour aggregates.
        assert_eq!(json["lastHour"]["samples"], 3);
        assert_eq!(json["lastHour"]["totalSpaces"]["min"], 2);
        assert_eq!(json["lastHour"]["totalSpaces"]["max"], 3);
        assert_eq!(json["lastHour"]["totalCells"]["min"], 10);
        assert_eq!(json["lastHour"]["totalCells"]["max"], 15);
        assert_eq!(json["lastHour"]["uniqueAgents"]["min"], 4);
        assert_eq!(json["lastHour"]["uniqueAgents"]["max"], 6);
        // (10 + 15 + 12) / (5 + 6 + 4)
        assert_eq!(
            json["lastHour"]["avgCellsPerAgent"],
            37.0 / 15.0,
        );

        // Verify allTime matches lastHour (same data so far).
        assert_eq!(json["allTime"]["samples"], 3);
        assert_eq!(json["allTime"]["totalCells"]["min"], 10);
        assert_eq!(json["allTime"]["totalCells"]["max"], 15);
    }

    #[test]
    fn metrics_collector_hour_cascade() {
        let mc = MetricsCollector::new();

        // Record 60 snapshots to trigger an hourly cascade.
        for i in 0..60 {
            mc.record_snapshot(stats(1, i + 1, i + 1, 1, i + 1));
        }

        let json = mc.to_json(stats(1, 60, 60, 1, 60));
        let obj = json.as_object().unwrap();

        // Should now have lastDay (hourly cascade happened).
        assert!(obj.contains_key("lastDay"));
        assert_eq!(json["lastDay"]["samples"], 60);
        assert_eq!(json["lastDay"]["totalCells"]["min"], 1);
        assert_eq!(json["lastDay"]["totalCells"]["max"], 60);
    }

    #[test]
    fn metrics_collector_day_cascade() {
        let mc = MetricsCollector::new();

        // 60 snapshots * 24 hours = 1440 snapshots to trigger a daily cascade.
        for i in 0..1440 {
            mc.record_snapshot(stats(1, i + 1, i + 1, 1, i + 1));
        }

        let json = mc.to_json(stats(1, 1440, 1440, 1, 1440));
        let obj = json.as_object().unwrap();

        assert!(obj.contains_key("lastWeek"));
        assert!(obj.contains_key("lastMonth"));
    }
}
