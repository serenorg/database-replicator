use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{BuildHasherDefault, Hasher};

use rustc_hash::FxHasher as InnerFxHasher;

pub struct FxHasher(InnerFxHasher);

impl Default for FxHasher {
    fn default() -> Self {
        Self(InnerFxHasher::default())
    }
}

impl Clone for FxHasher {
    fn clone(&self) -> Self {
        // FxHasher does not implement Clone; start a new hasher
        Self::default()
    }
}

impl fmt::Debug for FxHasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FxHasher").finish()
    }
}

impl Hasher for FxHasher {
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes)
    }
}

pub type FxBuildHasher = BuildHasherDefault<FxHasher>;
pub type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;
pub type FxHashSet<V> = HashSet<V, FxBuildHasher>;
