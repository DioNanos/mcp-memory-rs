use crate::error::Result;
use crate::store::Store;

pub fn search_categories(
    store: &Store,
    query: &str,
    limit: u32,
) -> Result<Vec<crate::store::SearchResult>> {
    store.search(query, limit)
}
