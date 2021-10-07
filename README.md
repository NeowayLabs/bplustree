# BPlusTree

<a href="https://docs.rs/bplustree"><img src="https://docs.rs/bplustree/badge.svg"></a>
<a href="https://crates.io/crates/bplustree"><img src="https://img.shields.io/crates/v/bplustree.svg"></a>

Implementation of a fast in-memory concurrent B+ Tree featuring optimistic lock coupling. The implementation
is based on [LeanStore](https://dbis1.github.io/leanstore.html) with some adaptations from
[Umbra](https://umbra-db.com/#publications).

The current API is very basic and more features are supposed to be added in the following
versions, it tries to loosely follow the `std::collections::BTreeMap` API.

Currently it is not heavily optimized but is already faster than some concurrent lock-free
implementations. Single threaded performance is generally slower (~ 1.4x) but still comparable to `std::collections::BTreeMap`
with sligthly faster scans due to the B+ Tree topology.

For how to use it refer to the [documentation](https://docs.rs/bplustree)
