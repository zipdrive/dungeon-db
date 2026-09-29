/// Extracts a list of OIDs from a string containing a comma-separated list.
pub fn oid_list<S>(oids: S) -> Vec<i64> where S : AsRef<str> {
    oids.as_ref().split(',')
        .filter_map(|s| match s.parse::<i64>() { Ok(i) => Some(i), Err(_) => None })
        .collect()
}