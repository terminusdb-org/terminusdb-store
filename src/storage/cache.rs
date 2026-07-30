use super::layer::*;
use crate::layer::*;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock, Weak};
use tdb_succinct::{StringDict, TypedDict};

pub trait LayerCache: 'static + Send + Sync {
    fn get_layer_from_cache(&self, name: [u32; 5]) -> Option<Arc<InternalLayer>>;
    fn cache_layer(&self, layer: Arc<InternalLayer>);

    fn invalidate(&self, name: [u32; 5]);

    /// Returns statistics about the cache: (total_entries, live_entries, dead_entries)
    /// Default implementation returns (0, 0, 0) for caches that don't track this.
    fn cache_stats(&self) -> (usize, usize, usize) {
        (0, 0, 0)
    }

    /// Remove stale entries from the cache. Returns number of entries removed.
    /// Default implementation does nothing.
    fn cleanup_stale_entries(&self) -> usize {
        0
    }

    /// Returns bytes of backing data: (total_bytes, live_bytes, dead_bytes).
    /// Default implementation returns (0, 0, 0).
    fn cache_memory_bytes(&self) -> (usize, usize, usize) {
        (0, 0, 0)
    }

    /// Returns all layer IDs currently in the cache.
    /// Default implementation returns an empty vector.
    fn cached_layer_ids(&self) -> Vec<[u32; 5]> {
        Vec::new()
    }

    /// Associate a cached layer with a database name for bulk invalidation.
    /// Default implementation does nothing.
    fn associate_with_database(&self, _name: [u32; 5], _database: &str) {}

    /// Invalidate all cached layers associated with a database.
    /// Returns the number of layers invalidated.
    /// Default implementation returns 0.
    fn invalidate_database(&self, _database: &str) -> usize {
        0
    }
}

pub struct NoCache;

impl LayerCache for NoCache {
    fn get_layer_from_cache(&self, _name: [u32; 5]) -> Option<Arc<InternalLayer>> {
        None
    }

    fn cache_layer(&self, _layer: Arc<InternalLayer>) {}

    fn invalidate(&self, _name: [u32; 5]) {}
}

lazy_static! {
    pub static ref NOCACHE: Arc<dyn LayerCache> = Arc::new(NoCache);
}

// locking isn't really ideal but the lock window will be relatively small so it shouldn't hurt performance too much except on heavy updates.
// ideally we should be using some concurrent hashmap implementation instead.

/// Threshold for automatic cleanup: when cache size exceeds this, trigger cleanup on next cache_layer call.
const CACHE_CLEANUP_THRESHOLD: usize = 100;

/// Only cleanup if dead entries exceed this percentage of total.
const DEAD_ENTRY_PERCENTAGE_THRESHOLD: usize = 20;

#[derive(Default)]
pub struct LockingHashMapLayerCache {
    cache: RwLock<HashMap<[u32; 5], Weak<InternalLayer>>>,
    db_index: RwLock<HashMap<String, HashSet<[u32; 5]>>>,
}

impl LockingHashMapLayerCache {
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns statistics about the cache: (total_entries, live_entries, dead_entries)
    /// Live entries are those where the Weak reference can still be upgraded.
    /// Dead entries are stale weak references that should be cleaned up.
    pub fn stats(&self) -> (usize, usize, usize) {
        let cache = self
            .cache
            .read()
            .expect("rwlock read should always succeed, but got poisoned");
        let total = cache.len();
        let live = cache.values().filter(|w| w.strong_count() > 0).count();
        let dead = total - live;
        (total, live, dead)
    }

    /// Remove all stale (dead) weak references from the cache.
    /// Returns the number of entries removed.
    pub fn cleanup_stale(&self) -> usize {
        let mut cache = self
            .cache
            .write()
            .expect("rwlock write should always succeed, but got poisoned");
        let before = cache.len();
        cache.retain(|_, weak| weak.strong_count() > 0);
        let after = cache.len();
        before - after
    }

    /// Returns (total_bytes, live_bytes, dead_bytes) of stored_size() across cache entries.
    pub fn memory_bytes(&self) -> (usize, usize, usize) {
        let cache = self
            .cache
            .read()
            .expect("rwlock read should always succeed, but got poisoned");
        let mut live = 0usize;
        let mut dead = 0usize;
        for w in cache.values() {
            if let Some(layer) = w.upgrade() {
                live += layer.stored_size();
            } else {
                // Dead entries have no accessible size, count as 0
                dead += 0;
            }
        }
        (live + dead, live, dead)
    }
}

impl LayerCache for LockingHashMapLayerCache {
    fn get_layer_from_cache(&self, name: [u32; 5]) -> Option<Arc<InternalLayer>> {
        let cache = self
            .cache
            .read()
            .expect("rwlock read should always succeed");

        let result = cache.get(&name).map(|c| c.to_owned());
        std::mem::drop(cache);

        match result {
            None => None,
            Some(weak) => match weak.upgrade() {
                None => {
                    self.cache
                        .write()
                        .expect("rwlock write should always succeed")
                        .remove(&name);
                    None
                }
                Some(result) => Some(result),
            },
        }
    }

    fn cache_layer(&self, layer: Arc<InternalLayer>) {
        let mut cache = self
            .cache
            .write()
            .expect("rwlock write should always succeed");

        // Automatic cleanup: when cache exceeds threshold, check for stale entries
        let cache_size = cache.len();
        if cache_size >= CACHE_CLEANUP_THRESHOLD {
            // Count dead entries
            let dead_count = cache.values().filter(|w| w.strong_count() == 0).count();
            let dead_percentage = (dead_count * 100) / cache_size.max(1);

            // Only cleanup if dead entries exceed threshold percentage
            if dead_percentage >= DEAD_ENTRY_PERCENTAGE_THRESHOLD {
                cache.retain(|_, weak| weak.strong_count() > 0);
            }
        }

        cache.insert(layer.name(), Arc::downgrade(&layer));
    }

    fn invalidate(&self, name: [u32; 5]) {
        let mut cache = self
            .cache
            .write()
            .expect("rwlock read should always succeed");

        cache.remove(&name);
    }

    fn cache_stats(&self) -> (usize, usize, usize) {
        self.stats()
    }

    fn cleanup_stale_entries(&self) -> usize {
        self.cleanup_stale()
    }

    fn cache_memory_bytes(&self) -> (usize, usize, usize) {
        self.memory_bytes()
    }

    fn cached_layer_ids(&self) -> Vec<[u32; 5]> {
        let cache = self
            .cache
            .read()
            .expect("rwlock read should always succeed, but got poisoned");
        cache.keys().copied().collect()
    }

    fn associate_with_database(&self, name: [u32; 5], database: &str) {
        let mut index = self
            .db_index
            .write()
            .expect("rwlock write should always succeed");
        index
            .entry(database.to_string())
            .or_insert_with(HashSet::new)
            .insert(name);
    }

    fn invalidate_database(&self, database: &str) -> usize {
        let ids = {
            let mut index = self
                .db_index
                .write()
                .expect("rwlock write should always succeed");
            index.remove(database)
        };
        match ids {
            Some(id_set) => {
                let count = id_set.len();
                for id in id_set {
                    self.invalidate(id);
                }
                count
            }
            None => 0,
        }
    }
}

#[derive(Clone)]
pub struct CachedLayerStore {
    pub(crate) inner: Arc<dyn LayerStore>,
    pub(crate) cache: Arc<dyn LayerCache>,
}

impl CachedLayerStore {
    pub fn new<S: LayerStore, C: LayerCache>(inner: S, cache: C) -> CachedLayerStore {
        CachedLayerStore {
            inner: Arc::new(inner),
            cache: Arc::new(cache),
        }
    }

    pub fn invalidate(&self, name: [u32; 5]) {
        self.cache.invalidate(name);
    }

    /// Returns cache statistics: (total_entries, live_entries, dead_entries)
    pub fn cache_stats(&self) -> (usize, usize, usize) {
        self.cache.cache_stats()
    }

    /// Remove stale entries from the cache. Returns number of entries removed.
    pub fn cleanup_stale_entries(&self) -> usize {
        self.cache.cleanup_stale_entries()
    }

    /// Returns bytes of backing data: (total_bytes, live_bytes, dead_bytes).
    pub fn cache_memory_bytes(&self) -> (usize, usize, usize) {
        self.cache.cache_memory_bytes()
    }

    /// Associate a cached layer with a database name for bulk invalidation.
    pub fn associate_with_database(&self, name: [u32; 5], database: &str) {
        self.cache.associate_with_database(name, database);
    }

    /// Invalidate all cached layers associated with a database.
    /// Returns the number of layers invalidated.
    pub fn invalidate_database(&self, database: &str) -> usize {
        self.cache.invalidate_database(database)
    }
}

#[async_trait]
impl LayerStore for CachedLayerStore {
    async fn layers(&self) -> io::Result<Vec<[u32; 5]>> {
        self.inner.layers().await
    }

    async fn get_layer(&self, name: [u32; 5]) -> io::Result<Option<Arc<InternalLayer>>> {
        self.inner
            .get_layer_with_cache(name, self.cache.clone())
            .await
    }

    async fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<Option<Arc<InternalLayer>>> {
        self.inner.get_layer_with_cache(name, cache).await
    }

    async fn finalize_layer(&self, name: [u32; 5]) -> io::Result<()> {
        self.inner.finalize_layer(name).await
    }

    async fn get_layer_parent_name(&self, name: [u32; 5]) -> io::Result<Option<[u32; 5]>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            Ok(layer.parent_name())
        } else {
            self.inner.get_layer_parent_name(name).await
        }
    }

    async fn get_node_dictionary(&self, name: [u32; 5]) -> io::Result<Option<StringDict>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            // unless it is a rollup
            if !layer.is_rollup() {
                return Ok(Some(layer.node_dictionary().clone()));
            }
        }

        self.inner.get_node_dictionary(name).await
    }

    async fn get_predicate_dictionary(&self, name: [u32; 5]) -> io::Result<Option<StringDict>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            // unless it is a rollup
            if !layer.is_rollup() {
                return Ok(Some(layer.predicate_dictionary().clone()));
            }
        }

        self.inner.get_predicate_dictionary(name).await
    }

    async fn get_value_dictionary(&self, name: [u32; 5]) -> io::Result<Option<TypedDict>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            // unless it is a rollup
            if !layer.is_rollup() {
                return Ok(Some(layer.value_dictionary().clone()));
            }
        }

        self.inner.get_value_dictionary(name).await
    }

    async fn get_node_count(&self, name: [u32; 5]) -> io::Result<Option<u64>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            // unless it is a rollup
            if !layer.is_rollup() {
                return Ok(Some(layer.node_dictionary().num_entries() as u64));
            }
        }

        self.inner.get_node_count(name).await
    }

    async fn get_predicate_count(&self, name: [u32; 5]) -> io::Result<Option<u64>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            // unless it is a rollup
            if !layer.is_rollup() {
                return Ok(Some(layer.predicate_dictionary().num_entries() as u64));
            }
        }

        self.inner.get_value_count(name).await
    }

    async fn get_value_count(&self, name: [u32; 5]) -> io::Result<Option<u64>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            // unless it is a rollup
            if !layer.is_rollup() {
                return Ok(Some(layer.value_dictionary().num_entries() as u64));
            }
        }

        self.inner.get_value_count(name).await
    }

    async fn get_node_value_idmap(&self, name: [u32; 5]) -> io::Result<Option<IdMap>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            // unless it is a rollup
            if !layer.is_rollup() {
                return Ok(Some(layer.node_value_id_map().clone()));
            }
        }

        self.inner.get_node_value_idmap(name).await
    }

    async fn get_predicate_idmap(&self, name: [u32; 5]) -> io::Result<Option<IdMap>> {
        // is layer in cache? if so, we can use the cached version
        if let Some(layer) = self.cache.get_layer_from_cache(name) {
            // unless it is a rollup
            if !layer.is_rollup() {
                return Ok(Some(layer.predicate_id_map().clone()));
            }
        }

        self.inner.get_predicate_idmap(name).await
    }

    async fn create_base_layer(&self) -> io::Result<Box<dyn LayerBuilder>> {
        self.inner.create_base_layer().await
    }

    async fn create_child_layer(&self, parent: [u32; 5]) -> io::Result<Box<dyn LayerBuilder>> {
        self.inner
            .create_child_layer_with_cache(parent, self.cache.clone())
            .await
    }

    async fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<Box<dyn LayerBuilder>> {
        self.inner
            .create_child_layer_with_cache(parent, cache)
            .await
    }

    async fn perform_rollup(&self, layer: Arc<InternalLayer>) -> io::Result<[u32; 5]> {
        self.inner.perform_rollup(layer).await
    }

    async fn perform_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<[u32; 5]> {
        self.inner
            .perform_rollup_upto_with_cache(layer, upto, cache)
            .await
    }

    async fn perform_rollup_upto(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
    ) -> io::Result<[u32; 5]> {
        self.inner
            .perform_rollup_upto_with_cache(layer, upto, self.cache.clone())
            .await
    }

    async fn perform_imprecise_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> io::Result<[u32; 5]> {
        self.inner
            .perform_imprecise_rollup_upto_with_cache(layer, upto, cache)
            .await
    }

    async fn perform_imprecise_rollup_upto(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
    ) -> io::Result<[u32; 5]> {
        self.inner
            .perform_imprecise_rollup_upto_with_cache(layer, upto, self.cache.clone())
            .await
    }

    async fn register_rollup(&self, layer: [u32; 5], rollup: [u32; 5]) -> io::Result<()> {
        // when registering a rollup layer, we need to make sure that
        // the cached version is updated as well.

        // Get the entire parent chain before registering the rollup
        // so we can invalidate all old layers from the cache
        let layer_stack = self.inner.retrieve_layer_stack_names(layer).await?;

        self.inner.register_rollup(layer, rollup).await?;

        // Invalidate the entire parent chain from cache, not just the rolled-up layer.
        // This ensures old layers become "dead" entries that can be cleaned up.
        // Without this, the old parent chain stays "live" because Arc references
        // are held by child layers.
        for old_layer in layer_stack {
            self.cache.invalidate(old_layer);
        }

        Ok(())
    }

    async fn rollup_upto(&self, layer: Arc<InternalLayer>, upto: [u32; 5]) -> io::Result<[u32; 5]> {
        let cache = self.cache.clone();
        self.rollup_upto_with_cache(layer, upto, cache).await
    }

    async fn squash(&self, layer: Arc<InternalLayer>) -> io::Result<[u32; 5]> {
        self.inner.squash(layer).await
    }

    async fn squash_upto(&self, layer: Arc<InternalLayer>, upto: [u32; 5]) -> io::Result<[u32; 5]> {
        self.inner.squash_upto(layer, upto).await
    }

    async fn merge_base_layer(&self, layers: &[[u32; 5]], temp_dir: &Path) -> io::Result<[u32; 5]> {
        self.inner.merge_base_layer(layers, temp_dir).await
    }

    async fn layer_is_ancestor_of(
        &self,
        descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> io::Result<bool> {
        self.inner.layer_is_ancestor_of(descendant, ancestor).await
    }

    async fn triple_addition_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_addition_exists(subject, predicate, object));
            }
        }

        self.inner
            .triple_addition_exists(layer, subject, predicate, object)
            .await
    }

    async fn triple_removal_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> io::Result<bool> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_removal_exists(subject, predicate, object));
            }
        }

        self.inner
            .triple_removal_exists(layer, subject, predicate, object)
            .await
    }

    async fn triple_additions(
        &self,
        layer: [u32; 5],
    ) -> io::Result<OptInternalLayerTripleSubjectIterator> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_additions());
            }
        }

        self.inner.triple_additions(layer).await
    }

    async fn triple_removals(
        &self,
        layer: [u32; 5],
    ) -> io::Result<OptInternalLayerTripleSubjectIterator> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_removals());
            }
        }

        self.inner.triple_removals(layer).await
    }

    async fn triple_additions_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_additions_s(subject));
            }
        }

        self.inner.triple_additions_s(layer, subject).await
    }

    async fn triple_removals_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_removals_s(subject));
            }
        }

        self.inner.triple_removals_s(layer, subject).await
    }

    async fn triple_additions_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_additions_sp(subject, predicate));
            }
        }

        self.inner
            .triple_additions_sp(layer, subject, predicate)
            .await
    }

    async fn triple_removals_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_removals_sp(subject, predicate));
            }
        }

        self.inner
            .triple_removals_sp(layer, subject, predicate)
            .await
    }

    async fn triple_additions_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(Box::new(cached.internal_triple_additions_p(predicate))
                    as Box<dyn Iterator<Item = _> + Send>);
            }
        }

        self.inner.triple_additions_p(layer, predicate).await
    }

    async fn triple_removals_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(Box::new(cached.internal_triple_removals_p(predicate))
                    as Box<dyn Iterator<Item = _> + Send>);
            }
        }

        self.inner.triple_removals_p(layer, predicate).await
    }

    async fn triple_additions_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_additions_o(object));
            }
        }

        self.inner.triple_additions_o(layer, object).await
    }

    async fn triple_removals_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> io::Result<Box<dyn Iterator<Item = IdTriple> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_removals_o(object));
            }
        }

        self.inner.triple_removals_o(layer, object).await
    }

    async fn triple_layer_addition_count(&self, layer: [u32; 5]) -> io::Result<usize> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_layer_addition_count());
            }
        }

        self.inner.triple_layer_addition_count(layer).await
    }

    async fn triple_layer_removal_count(&self, layer: [u32; 5]) -> io::Result<usize> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Ok(cached.internal_triple_layer_removal_count());
            }
        }

        self.inner.triple_layer_removal_count(layer).await
    }

    async fn retrieve_layer_stack_names(&self, name: [u32; 5]) -> io::Result<Vec<[u32; 5]>> {
        self.inner.retrieve_layer_stack_names(name).await
    }

    async fn retrieve_layer_stack_names_upto(
        &self,
        name: [u32; 5],
        upto: [u32; 5],
    ) -> io::Result<Vec<[u32; 5]>> {
        self.inner.retrieve_layer_stack_names_upto(name, upto).await
    }

    fn layer_cache_stats(&self) -> (usize, usize, usize) {
        self.cache.cache_stats()
    }

    fn cleanup_layer_cache(&self) -> usize {
        self.cache.cleanup_stale_entries()
    }

    fn layer_cache_memory_bytes(&self) -> (usize, usize, usize) {
        self.cache.cache_memory_bytes()
    }

    fn cached_layer_ids(&self) -> Vec<[u32; 5]> {
        self.cache.cached_layer_ids()
    }

    fn invalidate(&self, name: [u32; 5]) {
        self.cache.invalidate(name);
    }

    fn associate_with_database(&self, name: [u32; 5], database: &str) {
        self.cache.associate_with_database(name, database);
    }

    fn invalidate_database(&self, database: &str) -> usize {
        self.cache.invalidate_database(database)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::directory::*;
    use crate::storage::memory::*;
    use tempfile::tempdir;

    fn cached_layer_eq(layer1: &dyn Layer, layer2: &dyn Layer) -> bool {
        // a trait object consists of two parts, a pointer to the concrete data, followed by a vtable.
        // we consider two layers equal if that first part, the pointer to the concrete data, is equal.
        unsafe {
            let ptr1 = *(layer1 as *const dyn Layer as *const usize);
            let ptr2 = *(layer2 as *const dyn Layer as *const usize);
            ptr1 == ptr2
        }
    }

    #[tokio::test]
    async fn cached_memory_layer_store_returns_same_layer_multiple_times() {
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let layer1 = store.get_layer(child_name).await.unwrap().unwrap();
        let layer2 = store.get_layer(child_name).await.unwrap().unwrap();

        let base_layer = store.cache.get_layer_from_cache(base_name).unwrap();
        let base_layer_2 = store.get_layer(base_name).await.unwrap().unwrap();

        assert!(cached_layer_eq(&*layer1, &*layer2));
        assert!(cached_layer_eq(&*base_layer, &*base_layer_2));
    }

    #[tokio::test]
    async fn cached_directory_layer_store_returns_same_layer_multiple_times() {
        let dir = tempdir().unwrap();
        let store = CachedLayerStore::new(
            DirectoryLayerStore::new(dir.path()),
            LockingHashMapLayerCache::new(),
        );
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let layer1 = store.get_layer(child_name).await.unwrap().unwrap();
        let layer2 = store.get_layer(child_name).await.unwrap().unwrap();

        let base_layer = store.cache.get_layer_from_cache(base_name).unwrap();
        let base_layer_2 = store.get_layer(base_name).await.unwrap().unwrap();

        assert!(cached_layer_eq(&*layer1, &*layer2));
        assert!(cached_layer_eq(&*base_layer, &*base_layer_2));
    }

    #[tokio::test]
    async fn cached_layer_store_forgets_entries_when_they_are_dropped() {
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        let layer = store.get_layer(base_name).await.unwrap().unwrap();
        let weak = Arc::downgrade(&layer);

        // we expect 2 weak pointers, the one we made above and the one stored in cache
        assert_eq!(2, Arc::weak_count(&layer));

        // forget the layers
        std::mem::drop(layer);

        // according to our weak reference, there's no longer any strong reference around
        assert!(weak.upgrade().is_none());

        // retrieving the same layer again works just fine
        let layer = store.get_layer(base_name).await.unwrap().unwrap();

        // and only has one weak pointer pointing to it, the newly cached one
        assert_eq!(1, Arc::weak_count(&layer));
    }

    #[test]
    fn retrieve_layer_stack_names_retrieves_correctly() {
        //let store = CachedLayerStore::new(MemoryLayerStore::new());
        //let builder = store.create_base_layer().wait().unwrap();
    }

    #[test]
    fn associate_and_invalidate_database() {
        let cache = LockingHashMapLayerCache::new();
        let ids: [[u32; 5]; 3] = [
            [1, 2, 3, 4, 5],
            [6, 7, 8, 9, 10],
            [11, 12, 13, 14, 15],
        ];

        for id in &ids {
            cache.associate_with_database(*id, "admin|testdb");
        }

        let count = cache.invalidate_database("admin|testdb");
        assert_eq!(count, 3);
    }

    #[test]
    fn invalidate_nonexistent_database() {
        let cache = LockingHashMapLayerCache::new();
        let count = cache.invalidate_database("admin|nonexistent");
        assert_eq!(count, 0);
    }

    #[test]
    fn associate_multiple_databases() {
        let cache = LockingHashMapLayerCache::new();

        cache.associate_with_database([1, 2, 3, 4, 5], "admin|db1");
        cache.associate_with_database([6, 7, 8, 9, 10], "admin|db1");
        cache.associate_with_database([11, 12, 13, 14, 15], "admin|db2");
        cache.associate_with_database([16, 17, 18, 19, 20], "admin|db2");

        let count = cache.invalidate_database("admin|db1");
        assert_eq!(count, 2);

        // db2 associations should still be present
        let count2 = cache.invalidate_database("admin|db2");
        assert_eq!(count2, 2);
    }

    #[test]
    fn invalidate_does_not_affect_unassociated() {
        let cache = LockingHashMapLayerCache::new();

        cache.associate_with_database([1, 2, 3, 4, 5], "admin|db1");
        cache.associate_with_database([6, 7, 8, 9, 10], "admin|db1");

        // Associate a different ID with a different database
        cache.associate_with_database([11, 12, 13, 14, 15], "admin|db2");

        let count = cache.invalidate_database("admin|db1");
        assert_eq!(count, 2);

        // db2 should still have its association
        let count2 = cache.invalidate_database("admin|db2");
        assert_eq!(count2, 1);
    }
}
