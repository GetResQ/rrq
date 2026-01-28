use std::collections::HashMap;

use rrq::constants::QUEUE_KEY_PREFIX;

pub(crate) fn queue_matches(filter: &str, job_queue: &str) -> bool {
    if filter == job_queue {
        return true;
    }
    if job_queue == format!("{}{}", QUEUE_KEY_PREFIX, filter) {
        return true;
    }
    false
}

pub(crate) fn top_counts(map: &HashMap<String, usize>, limit: usize) -> Vec<(String, usize)> {
    let mut items = map.iter().map(|(k, v)| (k.clone(), *v)).collect::<Vec<_>>();
    items.sort_by(|a, b| b.1.cmp(&a.1));
    items.into_iter().take(limit).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_matches_accepts_exact_and_prefixed() {
        assert!(queue_matches("default", "default"));
        assert!(queue_matches("default", "rrq:queue:default"));
        assert!(!queue_matches("default", "other"));
    }

    #[test]
    fn top_counts_orders_and_limits() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 3);
        map.insert("c".to_string(), 2);
        let top = top_counts(&map, 2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "b");
        assert_eq!(top[1].0, "c");
    }
}
