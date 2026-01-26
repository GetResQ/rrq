use std::collections::HashMap;

use rrq_orchestrator::constants::QUEUE_KEY_PREFIX;

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
