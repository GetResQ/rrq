use crate::defaults::QUEUE_KEY_PREFIX;

#[must_use]
pub fn normalize_queue_name(queue_name: &str) -> String {
    if queue_name.starts_with(QUEUE_KEY_PREFIX) {
        queue_name.to_string()
    } else {
        format!("{QUEUE_KEY_PREFIX}{queue_name}")
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_queue_name;

    #[test]
    fn normalize_queue_name_adds_prefix_for_bare_names() {
        assert_eq!(normalize_queue_name("default"), "rrq:queue:default");
    }

    #[test]
    fn normalize_queue_name_preserves_prefixed_names() {
        assert_eq!(
            normalize_queue_name("rrq:queue:mail-ingest"),
            "rrq:queue:mail-ingest"
        );
    }
}
