//! Bounded maps: what the LAN says can grow without limit, what this node
//! remembers about it cannot.

use std::collections::HashMap;
use std::hash::Hash;

/// Shrink `map` to at most `cap` entries by removing, one at a time, the
/// entry whose `rank` is greatest. Entries ranked `None` are never
/// removed. Returns the removed keys.
pub fn evict_past_cap<K, V, R>(
    map: &mut HashMap<K, V>,
    cap: usize,
    mut rank: impl FnMut(&K, &V) -> Option<R>,
) -> Vec<K>
where
    K: Clone + Eq + Hash,
    R: Ord,
{
    let mut evicted = Vec::new();
    while map.len() > cap {
        let Some(victim) = map
            .iter()
            .filter_map(|(k, v)| rank(k, v).map(|r| (r, k)))
            .max_by(|a, b| a.0.cmp(&b.0))
            .map(|(_, k)| k.clone())
        else {
            break;
        };
        map.remove(&victim);
        evicted.push(victim);
    }
    evicted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_the_highest_ranked_until_within_the_cap() {
        let mut map: HashMap<u8, u8> = (0..5).map(|i| (i, i)).collect();
        let evicted = evict_past_cap(&mut map, 3, |k, _| Some(*k));
        assert_eq!(evicted, vec![4, 3]);
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn unranked_entries_are_kept_even_past_the_cap() {
        let mut map: HashMap<u8, u8> = (0..5).map(|i| (i, i)).collect();
        let evicted =
            evict_past_cap(&mut map, 1, |k, _| (*k < 2).then_some(*k));
        assert_eq!(evicted, vec![1, 0]);
        assert_eq!(map.len(), 3, "the unranked stay");
    }
}
