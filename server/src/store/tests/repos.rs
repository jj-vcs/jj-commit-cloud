// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::super::*;

#[test]
fn test_bounded_namespace_cache_evicts_oldest_entries() {
    let mut cache = BoundedNamespaceCache::with_capacity(2);
    cache.insert("a".to_string(), 1);
    cache.insert("b".to_string(), 2);
    cache.insert("c".to_string(), 3);
    assert!(!cache.contains("a"));
    assert_eq!(cache.get("b"), Some(&2));
    assert_eq!(cache.get("c"), Some(&3));
}
