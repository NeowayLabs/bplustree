use bplustree::latch::{HybridLatch, OptimisticGuard, SharedGuard, ExclusiveGuard, HybridGuard};
use bplustree::error;
use super::node::{Node, NodeKind, LeafNode, InternalNode};
use super::bufmgr::{swip::Swip, OptSwipGuard, ShrSwipGuard, ExvSwipGuard, BufferFrame};
use super::bufmgr;

use std::sync::atomic::{AtomicUsize, Ordering};

pub struct PersistentBPlusTree {
    root: HybridLatch<Swip<HybridLatch<BufferFrame<Node>>>>,
    height: AtomicUsize
}

pub(crate) enum ParentHandler<'t> {
    Root { tree_guard: OptimisticGuard<'t, Swip<HybridLatch<BufferFrame<Node>>>> },
    Parent {
        parent_guard: OptNodeGuard,
        pos: usize
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) enum Direction {
    Forward,
    Reverse
}

macro_rules! tp {
    ($x:expr) => {
        println!("[{:?}] {}", std::thread::current().id(), format!($x));
    };
    ($x:expr, $($y:expr),+) => {
        println!("[{:?}] {}", std::thread::current().id(), format!($x, $($y),+));
    };
}

pub type OptNodeGuard = OptimisticGuard<'static, Node, BufferFrame<Node>>;
pub type ShrNodeGuard = SharedGuard<'static, Node, BufferFrame<Node>>;
pub type ExvNodeGuard = ExclusiveGuard<'static, Node, BufferFrame<Node>>;

fn allocate_leaf_for(size: usize) -> ExvNodeGuard {
    let (mut bf_guard, capacity) = bufmgr().allocate_page_for(size).expect("failed to allocate leaf");
    let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| &mut bf.page.value);
    node_guard.init(true, capacity);
    node_guard
}

fn allocate_internal_for(size: usize) -> ExvNodeGuard {
    let (mut bf_guard, capacity) = bufmgr().allocate_page_for(size).expect("failed to allocate internal");
    let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| &mut bf.page.value);
    node_guard.init(false, capacity);
    node_guard
}

pub(crate) fn bf_to_node_guard(guard: OptimisticGuard<'static, BufferFrame<Node>>) -> OptNodeGuard {
    OptimisticGuard::map(guard, |bf| Ok(&bf.page.value)).unwrap()
}

pub(crate) fn swip_to_node_guard(guard: OptSwipGuard<'static, Node>) -> OptNodeGuard {
    bf_to_node_guard(OptimisticGuard::unmap(guard))
}

fn retry<T, F: Fn() -> error::Result<T>>(f: F) -> T {
    loop {
        match f() {
            Ok(tup) => {
                return tup;
            }
            Err(_) => {
                // TODO backoff
                continue;
            }
        }
    }
}

impl PersistentBPlusTree {
    pub fn new() -> Self {
        let node_guard = allocate_leaf_for(1);

        PersistentBPlusTree {
            root: HybridLatch::new(Swip::from_ref(node_guard.latch())),
            height: AtomicUsize::new(1)
        }
    }

    /// Returns the height of the tree
    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    pub(crate) fn lock_coupling(swip_guard: OptSwipGuard<'static, Node>) -> error::Result<(OptSwipGuard<'static, Node>, OptNodeGuard)> {
        let (swip_guard, latch) = bufmgr().resolve_swip_fast(swip_guard)?;
        let bf_guard = latch.optimistic_or_spin();
        let node_guard = bf_to_node_guard(bf_guard);
        swip_guard.recheck()?;
        Ok((swip_guard, node_guard))
    }

    pub(crate) fn find_parent(&self, needle: &impl HybridGuard<Node, BufferFrame<Node>>) -> error::Result<ParentHandler> {
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = tree_guard.as_ref();
        let root_latch_ptr = root_latch as *const _;
        let root_guard = bf_to_node_guard(root_latch.optimistic_or_spin());

        if needle.latch() as *const _ == root_latch_ptr {
            tree_guard.recheck()?;
            return Ok(ParentHandler::Root { tree_guard })
        }

        enum SearchParam<'a> {
            Key(&'a [u8]),
            Infinity
        }

        let param = match needle.inner().upper_fence()? {
            Some(key) => SearchParam::Key(key),
            None => SearchParam::Infinity
        };

        let mut t_guard = Some(tree_guard);
        let mut p_guard: Option<OptNodeGuard> = None;
        let mut target_guard = root_guard;

        loop {
            let (swip_guard, pos) = match target_guard.downcast() {
                NodeKind::Internal(ref internal) => {
                    match param {
                        SearchParam::Key(key) => {
                            let (pos, _) = internal.lower_bound(key)?;
                            let swip_guard = OptimisticGuard::map(target_guard, |node| node.as_internal().edge_at(pos))?;
                            // let swip = internal.edge_at(pos)?;

                            (swip_guard, pos)
                        }
                        SearchParam::Infinity => {
                            let pos = internal.base.len();
                            // let swip = internal.upper_edge()?;
                            let swip_guard = OptimisticGuard::map(target_guard, |node| node.as_internal().upper_edge())?;
                            (swip_guard, pos)
                        }
                    }
                }
                NodeKind::Leaf(ref _leaf) => {
                    if let Some(p) = p_guard.as_ref() {
                        p.recheck()?;
                    }

                    target_guard.recheck()?;

                    needle.recheck()?; // This is needed to ensure this node was not merged during the search

                    if let Some(tree_guard) = t_guard {
                        tree_guard.recheck()?;
                    }

                    panic!("reaching leaves, merges or splits are wrong");
                    // return Err(error::Error::Unwind);
                }
            };

            if !swip_guard.swizzled() { // isEvicted
                // There may not be any evicted swip in the root to needle path
                return Err(error::Error::Unwind);
            }

            if swip_guard.as_raw() == needle.latch() as *const _ as u64 {
                swip_guard.recheck()?;
                return Ok(ParentHandler::Parent {
                    parent_guard: swip_to_node_guard(swip_guard),
                    pos
                });
            }

            let (swip_guard, node_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
            p_guard = Some(swip_to_node_guard(swip_guard));
            target_guard = node_guard;

            if let Some(tree_guard) = t_guard.take() {
                tree_guard.recheck()?;
            }
        }
    }

    fn find_leaf_and_parent<K: AsRef<[u8]>>(&self, key: K) -> error::Result<(OptNodeGuard, Option<(OptNodeGuard, usize)>)> {
        let key = key.as_ref();
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = tree_guard.as_ref();
        let root_latch_ptr = root_latch as *const _;
        let root_guard = bf_to_node_guard(root_latch.optimistic_or_spin());
        tree_guard.recheck()?;

        let mut t_guard = Some(tree_guard);
        let mut p_guard = None;
        let mut target_guard = root_guard;

        let mut level = 0u16;

        let leaf_guard = loop {
            let (swip_guard, pos) = match target_guard.downcast() {
                NodeKind::Internal(ref internal) => {
                    let (pos, _) = internal.lower_bound(key)?;
                    // tp!("found leaf and parent {} {} {}", level, pos, internal.base.len());
                    let swip_guard = OptimisticGuard::map(target_guard, |node| node.as_internal().edge_at(pos))?;
                    (swip_guard, pos)
                }
                NodeKind::Leaf(ref _leaf) => {
                    // tp!("found leaf");
                    break target_guard;
                }
            };

            // TODO if level == height - 1 use shared mode

            let (swip_guard, node_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
            // tp!("swizzled {}", pos);
            p_guard = Some((swip_to_node_guard(swip_guard), pos));
            target_guard = node_guard;

            if let Some(tree_guard) = t_guard.take() {
                tree_guard.recheck()?;
            }

            level += 1;
        };

        Ok((leaf_guard, p_guard))
    }

    fn find_leaf<K: AsRef<[u8]>>(&self, key: K) -> error::Result<OptNodeGuard> {
        let (leaf, _) = self.find_leaf_and_parent(key)?;
        Ok(leaf)
    }

    fn find_leaf_and_parent_from_node(&self, needle: OptNodeGuard, direction: Direction) -> error::Result<(OptNodeGuard, Option<(OptNodeGuard, usize)>)> {
        let mut p_guard = None;
        let mut target_guard = needle;

        let leaf_guard = loop {
            let (swip_guard, pos) = match target_guard.downcast() {
                NodeKind::Internal(ref internal) => {
                    let pos = match direction {
                        Direction::Forward => 0,
                        Direction::Reverse => internal.base.len()
                    };
                    let swip_guard = OptimisticGuard::map(target_guard, |node| node.as_internal().edge_at(pos))?;
                    (swip_guard, pos)
                }
                NodeKind::Leaf(ref _leaf) => {
                    break target_guard;
                }
            };

            let (swip_guard, node_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
            p_guard = Some((swip_to_node_guard(swip_guard), pos));
            target_guard = node_guard;
        };

        leaf_guard.recheck()?;

        Ok((leaf_guard, p_guard))
    }

    fn find_first_leaf_and_parent(&self) -> error::Result<(OptNodeGuard, Option<(OptNodeGuard, usize)>)> {
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = tree_guard.as_ref();
        let root_guard = bf_to_node_guard(root_latch.optimistic_or_spin());
        tree_guard.recheck()?;

        self.find_leaf_and_parent_from_node(root_guard, Direction::Forward)
    }

    fn find_last_leaf_and_parent(&self) -> error::Result<(OptNodeGuard, Option<(OptNodeGuard, usize)>)> {
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = tree_guard.as_ref();
        let root_guard = bf_to_node_guard(root_latch.optimistic_or_spin());
        tree_guard.recheck()?;

        self.find_leaf_and_parent_from_node(root_guard, Direction::Reverse)
    }

    pub(crate) fn find_first_shared_leaf_and_optimistic_parent(&self) -> (ShrNodeGuard, Option<(OptNodeGuard, usize)>) {
        retry(|| {
            let (leaf, parent_opt) = self.find_first_leaf_and_parent()?;
            let shared_leaf = leaf.to_shared()?;
            Ok((shared_leaf, parent_opt))
        })
    }

    pub(crate) fn find_last_shared_leaf_and_optimistic_parent(&self) -> (ShrNodeGuard, Option<(OptNodeGuard, usize)>) {
        retry(|| {
            let (leaf, parent_opt) = self.find_last_leaf_and_parent()?;
            let shared_leaf = leaf.to_shared()?;
            Ok((shared_leaf, parent_opt))
        })
    }

    pub(crate) fn find_shared_leaf_and_optimistic_parent<K: AsRef<[u8]>>(&self, key: K) -> (ShrNodeGuard, Option<(OptNodeGuard, usize)>) {
        let key = key.as_ref();
        retry(|| {
            // TODO try to make implementation specialized using tree height
            let (leaf, parent_opt) = self.find_leaf_and_parent(key)?;
            let shared_leaf = leaf.to_shared()?;
            Ok((shared_leaf, parent_opt))
        })
    }

    pub(crate) fn find_first_exclusive_leaf_and_optimistic_parent(&self) -> (ExvNodeGuard, Option<(OptNodeGuard, usize)>) {
        retry(|| {
            let (leaf, parent_opt) = self.find_first_leaf_and_parent()?;
            let exclusive_leaf = leaf.to_exclusive()?;
            Ok((exclusive_leaf, parent_opt))
        })
    }

    pub(crate) fn find_last_exclusive_leaf_and_optimistic_parent(&self) -> (ExvNodeGuard, Option<(OptNodeGuard, usize)>) {
        retry(|| {
            let (leaf, parent_opt) = self.find_last_leaf_and_parent()?;
            let exclusive_leaf = leaf.to_exclusive()?;
            Ok((exclusive_leaf, parent_opt))
        })
    }

    pub(crate) fn find_exact_exclusive_leaf_and_optimistic_parent<K: AsRef<[u8]>>(&self, key: K) -> Option<((ExvNodeGuard, usize), Option<(OptNodeGuard, usize)>)> {
        let key = key.as_ref();
        retry(|| {
            let (leaf, parent_opt) = self.find_leaf_and_parent(key)?;
            let (pos, exact) = leaf.as_leaf().lower_bound(key)?;
            if exact {
                let exclusive_leaf = leaf.to_exclusive()?;
                Ok(Some(((exclusive_leaf, pos), parent_opt)))
            } else {
                leaf.recheck()?;
                Ok(None)
            }
        })
    }

    pub(crate) fn find_exclusive_leaf_and_optimistic_parent<K: AsRef<[u8]>>(&self, key: K) -> (ExvNodeGuard, Option<(OptNodeGuard, usize)>) {
        let key = key.as_ref();
        retry(|| {
            // TODO try to make implementation specialized using tree height
            let (leaf, parent_opt) = self.find_leaf_and_parent(key)?;
            let exclusive_leaf = leaf.to_exclusive()?;
            Ok((exclusive_leaf, parent_opt))
        })
    }

    pub fn lookup<K, R, F>(&self, key: K, f: F) -> Option<R>
    where
        K: AsRef<[u8]>,
        F: Fn(&[u8]) -> R
    {
        let key = key.as_ref();
        retry(|| {
            let guard = self.find_leaf(key)?;
            if let NodeKind::Leaf(ref leaf) = guard.downcast() {
                let (pos, exact) = leaf.lower_bound(key)?;
                if exact {
                    let result = f(leaf.value_at(pos)?);
                    guard.recheck()?;
                    Ok(Some(result))
                } else {
                    guard.recheck()?;
                    Ok(None)
                }
            } else {
                unreachable!("must be a leaf node");
            }
        })
    }

    pub fn remove<K: AsRef<[u8]>>(&self, key: K) {
        if let Some(((mut guard, pos), _parent_opt)) = self.find_exact_exclusive_leaf_and_optimistic_parent(key) {
            guard.as_leaf_mut().remove_at(pos);

            if guard.is_underfull() {
                let guard = guard.unlock();
                loop {
                    let perform_merge = || {
                        let _ = self.try_merge(&guard)?;
                        error::Result::Ok(())
                    };

                    match perform_merge() {
                        Ok(_) => {
                            break;
                        },
                        Err(error::Error::Reclaimed) => { // TODO check if this is still emmited
                            break;
                        }
                        Err(_) => {
                            break; // TODO not ensuring merges happen timely
                            // guard = guard.latch().optimistic_or_spin();
                            // continue
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn try_split(&self, needle: &OptNodeGuard) -> error::Result<()> {
        // tp!("trysplit");
        let parent_handler = self.find_parent(needle)?;

        match parent_handler {
            ParentHandler::Root { tree_guard } => {
                let mut tree_guard_x = tree_guard.to_exclusive()?; // TODO tree root should not need this lock (use atomic store)

                let root_latch = tree_guard_x.as_ref();

                let mut root_guard_x = ExclusiveGuard::map(root_latch.exclusive(), |bf| &mut bf.page.value);

                // TODO assert!(height == 1 || !root_guard_x.is_leaf());

                let mut new_root_guard_x = allocate_internal_for(1); // TODO set some marker so it does not get evicted

                match root_guard_x.downcast_mut() {
                    NodeKind::Internal(root_internal_node) => {
                        if root_internal_node.base.len() <= 2 {
                            return Ok(())
                        }

                        let split_pos = root_internal_node.base.len() / 2; // TODO choose a better split position if bulk loading
                        let split_key = root_internal_node.full_key_at(split_pos).expect("should exist");

                        let mut new_right_node_guard_x = allocate_internal_for(1); // Think about different size classes

                        {
                            let new_right_node = new_right_node_guard_x.as_internal_mut();
                            root_internal_node.split(new_right_node, split_pos);
                        }

                        let old_root_edge = Swip::clone(&tree_guard_x);
                        let new_right_node_edge = Swip::from_ref(new_right_node_guard_x.latch());

                        {
                            let new_root = new_root_guard_x.as_internal_mut();
                            new_root.insert(split_key, old_root_edge).expect("must have space");
                            new_root.set_upper_edge(new_right_node_edge);
                        }
                    }
                    NodeKind::Leaf(root_leaf_node) => {
                        if root_leaf_node.base.len() <= 2 {
                            return Ok(())
                        }

                        let split_pos = root_leaf_node.base.len() / 2; // TODO choose a better split position if bulk loading
                        let split_key = root_leaf_node.full_key_at(split_pos).expect("should exist");

                        let mut new_right_node_guard_x = allocate_leaf_for(1); // Think about different size classes

                        {
                            let new_right_node = new_right_node_guard_x.as_leaf_mut();
                            root_leaf_node.split(new_right_node, split_pos);
                        }

                        let old_root_edge = Swip::clone(&tree_guard_x);
                        let new_right_node_edge = Swip::from_ref(new_right_node_guard_x.latch());

                        {
                            let new_root = new_root_guard_x.as_internal_mut();
                            new_root.insert(split_key, old_root_edge).expect("must have space");
                            new_root.set_upper_edge(new_right_node_edge);
                        }
                    }
                }

                let new_root_node_edge = Swip::from_ref(new_root_guard_x.latch());
                *tree_guard_x = new_root_node_edge;
                self.height.fetch_add(1, Ordering::Relaxed);
            },
            ParentHandler::Parent { parent_guard, pos } => {
                // tp!("parent");
                let swip_guard = OptimisticGuard::map(parent_guard, |node| node.as_internal().edge_at(pos))?;
                let (swip_guard, target_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                let parent_guard = swip_to_node_guard(swip_guard);

                let split_pos = target_guard.len() / 2; // TODO choose a better split position if bulk loading
                let split_key = match target_guard.downcast() {
                    NodeKind::Leaf(leaf) => leaf.full_key_at(split_pos)?,
                    NodeKind::Internal(internal) => internal.full_key_at(split_pos)?
                };

                let space_needed = parent_guard.as_internal().space_needed(split_key.len());
                if parent_guard.as_internal().has_enough_space_for(space_needed) {
                    let mut parent_guard_x = parent_guard.to_exclusive()?;
                    let mut target_guard_x = target_guard.to_exclusive()?;

                    match target_guard_x.downcast_mut() {
                        NodeKind::Internal(left_internal) => {
                            if left_internal.base.len() <= 2 {
                                return Ok(())
                            }

                            let mut new_right_node_guard_x = allocate_internal_for(1);

                            {
                                let new_right_node = new_right_node_guard_x.as_internal_mut();
                                left_internal.split(new_right_node, split_pos);
                            }

                            let new_right_node_edge = Swip::from_ref(new_right_node_guard_x.latch());

                            let parent_internal = parent_guard_x.as_internal_mut();

                            if pos == parent_internal.base.len() {
                                let left_edge = parent_internal.replace_upper_edge(new_right_node_edge);
                                parent_internal.insert(split_key, left_edge);
                            } else {
                                let left_edge = parent_internal.replace_edge_at(pos, new_right_node_edge);
                                parent_internal.insert(split_key, left_edge);
                            }
                        }
                        NodeKind::Leaf(left_leaf) => {
                            if left_leaf.base.len() <= 2 {
                                return Ok(())
                            }

                            let mut new_right_node_guard_x = allocate_leaf_for(1);

                            {
                                let new_right_node = new_right_node_guard_x.as_leaf_mut();
                                left_leaf.split(new_right_node, split_pos);
                            }

                            let new_right_node_edge = Swip::from_ref(new_right_node_guard_x.latch());

                            let parent_internal = parent_guard_x.as_internal_mut();

                            if pos == parent_internal.base.len() {
                                let left_edge = parent_internal.replace_upper_edge(new_right_node_edge);
                                parent_internal.insert(split_key, left_edge);
                            } else {
                                let left_edge = parent_internal.replace_edge_at(pos, new_right_node_edge);
                                parent_internal.insert(split_key, left_edge);
                            }
                        }
                    }
                } else {
                    self.try_split(&parent_guard)?;
                }
            }
        }
        // tp!("done");

        Ok(())
    }

    pub(crate) fn try_merge(&self, needle: &OptNodeGuard) -> error::Result<bool> {
        let parent_handler = self.find_parent(needle)?;

        match parent_handler {
            ParentHandler::Root { tree_guard: _ } => {
                return Ok(false);
            },
            ParentHandler::Parent { mut parent_guard, pos } => {
                let parent_len = parent_guard.len();

                let swip_guard = OptimisticGuard::map(parent_guard, |node| node.as_internal().edge_at(pos))?;
                let (swip_guard, mut target_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                let mut parent_guard = swip_to_node_guard(swip_guard);

                if !target_guard.is_underfull() { // TODO is_underfull
                    target_guard.recheck()?;
                    return Ok(false);
                }

                let merge_succeded = if parent_len > 1 && pos > 0 {
                    // Try merge left
                    let swip_guard = OptimisticGuard::map(parent_guard, |node| node.as_internal().edge_at(pos -1))?;
                    let (swip_guard, left_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                    parent_guard = swip_to_node_guard(swip_guard);

                    if !(left_guard.can_merge_with(&target_guard)) {
                        left_guard.recheck()?;
                        target_guard.recheck()?;
                        false
                    } else {
                        let mut parent_guard_x = parent_guard.to_exclusive()?;
                        let mut target_guard_x = target_guard.to_exclusive()?;
                        let mut left_guard_x = left_guard.to_exclusive()?;

                        match target_guard_x.downcast_mut() {
                            NodeKind::Leaf(ref mut target_leaf) => {
                                assert!(left_guard_x.is_leaf());

                                if !left_guard_x.as_leaf_mut().merge(target_leaf) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos - 1, left_edge);
                                    }

                                    // reclaim
                                    bufmgr().reclaim_page(ExclusiveGuard::unmap(target_guard_x));

                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = left_guard_x.unlock(); // FIXME moving left_guard into target_guard because
                                                                          // borrow checker requires target_guard to be valid
                                                                          // even in this case where it will not get used anymore
                                    true
                                }
                            }
                            NodeKind::Internal(target_internal) => {
                                assert!(!left_guard_x.is_leaf());

                                if !left_guard_x.as_internal_mut().merge(target_internal) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos -1, left_edge);
                                    }

                                    // reclaim
                                    bufmgr().reclaim_page(ExclusiveGuard::unmap(target_guard_x));

                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = left_guard_x.unlock(); // FIXME moving left_guard into target_guard because
                                                                          // borrow checker requires target_guard to be valid
                                                                          // even in this case where it will not get used anymore
                                    true
                                }
                            }
                        }
                    }
                } else {
                    false
                };

                let merge_succeded = if !merge_succeded && parent_len > 0 && (pos + 1) <= parent_len {
                    // Try merge right
                    let swip_guard = OptimisticGuard::map(parent_guard, |node| node.as_internal().edge_at(pos + 1))?;
                    let (swip_guard, right_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                    parent_guard = swip_to_node_guard(swip_guard);

                    if !(right_guard.can_merge_with(&target_guard)) {
                        right_guard.recheck()?;
                        target_guard.recheck()?;
                        false
                    } else {
                        let mut parent_guard_x = parent_guard.to_exclusive()?;
                        let mut target_guard_x = target_guard.to_exclusive()?;
                        let mut right_guard_x = right_guard.to_exclusive()?;

                        match target_guard_x.downcast_mut() {
                            NodeKind::Leaf(ref mut target_leaf) => {
                                assert!(right_guard_x.is_leaf());

                                if !target_leaf.merge(right_guard_x.as_leaf_mut()) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos + 1 == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {
                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos, left_edge);
                                    }

                                    // reclaim
                                    bufmgr().reclaim_page(ExclusiveGuard::unmap(right_guard_x));

                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    true
                                }
                            }
                            NodeKind::Internal(target_internal) => {
                                assert!(!right_guard_x.is_leaf());

                                if !target_internal.merge(right_guard_x.as_internal_mut()) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos + 1 == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {
                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos, left_edge);
                                    }

                                    // reclaim
                                    bufmgr().reclaim_page(ExclusiveGuard::unmap(right_guard_x));

                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    true
                                }
                            }
                        }
                    }
                } else {
                    merge_succeded
                };

                let parent_merge = || { // TODO test if we should ensure parent merges always happen
                    if parent_guard.is_underfull() {
                        parent_guard.recheck()?;
                        let _ = self.try_merge(&parent_guard)?;
                    }
                    error::Result::Ok(())
                };

                let _ = parent_merge();

                return Ok(merge_succeded)
            }
        }
    }

    pub fn raw_iter<'t>(&'t self) -> crate::iter::RawSharedIter<'t> {
        crate::iter::RawSharedIter::new(self)
    }

    pub fn raw_iter_mut<'t>(&'t self) -> crate::iter::RawExclusiveIter<'t> {
        crate::iter::RawExclusiveIter::new(self)
    }
}


#[cfg(test)]
mod tests {
    use super::{PersistentBPlusTree};
    use crate::ensure_global_bufmgr;
    use serial_test::serial;

    #[test]
    #[serial]
    fn persistent_bplustree_init() {
        ensure_global_bufmgr("/tmp/state.db", 1 * 1024 * 1024).unwrap();
        let tree = PersistentBPlusTree::new();
    }
}
