use crossbeam_epoch::{self as epoch, Atomic, Owned};
use smallvec::{smallvec, SmallVec};

use std::borrow::Borrow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::fmt;

// use parking_lot_core::SpinWait;

pub mod error;
pub mod latch;
pub mod iter;
#[cfg(test)]
pub mod util;
#[cfg(test)]
pub mod bench;

use latch::{HybridLatch, OptimisticGuard, SharedGuard, ExclusiveGuard, HybridGuard};

pub type BPlusTree<K, V> = GenericBPlusTree<K, V, 128, 256>;

pub struct GenericBPlusTree<K, V, const IC: usize, const LC: usize> {
    root: HybridLatch<Atomic<HybridLatch<Node<K, V, IC, LC>>>>,
    height: AtomicUsize
}

pub(crate) enum ParentHandler<'r, 'p, K, V, const IC: usize, const LC: usize> {
    Root { tree_guard: OptimisticGuard<'r, Atomic<HybridLatch<Node<K, V, IC, LC>>>> },
    Parent {
        parent_guard: OptimisticGuard<'p, Node<K, V, IC, LC>>,
        pos: u16
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

impl<K: Clone + Ord, V, const IC: usize, const LC: usize> GenericBPlusTree<K, V, IC, LC> {
    pub fn new() -> Self {
        GenericBPlusTree {
            root: HybridLatch::new(Atomic::new(HybridLatch::new(Node::Leaf(
                LeafNode {
                    len: 0,
                    keys: smallvec![],
                    values: smallvec![],
                    lower_fence: None,
                    upper_fence: None,
                    sample_key: None
                }
            )))),
            height: AtomicUsize::new(1)
        }
    }

    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    pub(crate) fn find_parent<'t, 'g, 'e>(&'t self, needle: &impl HybridGuard<Node<K, V, IC, LC>>, eg: &'e epoch::Guard) -> error::Result<ParentHandler<'t, 'e, K, V, IC, LC>>
    where
        K: Ord
    {
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = unsafe { tree_guard.load(Ordering::Acquire, eg).deref() };
        let root_latch_ptr = root_latch as *const _;
        let root_guard = root_latch.optimistic_or_spin();

        if needle.latch() as *const _ == root_latch_ptr {
            tree_guard.recheck()?;
            return Ok(ParentHandler::Root { tree_guard })
        }



        let search_key = match needle.inner().sample_key().cloned() {
            Some(key) => key,
            None => {
                needle.recheck()?;
                // This is not the root node, and does not have a sample key. Assume that it is being
                // reclaimed
                return Err(error::Error::Reclaimed);
            }
        };

        let mut t_guard = Some(tree_guard);
        let mut p_guard: Option<OptimisticGuard<'e, Node<K, V, IC, LC>>> = None;
        let mut target_guard = root_guard;

        let (c_swip, pos) = loop {
            let (c_swip, pos) = match *target_guard {
                Node::Internal(ref internal) => {
                    let (pos, _) = internal.lower_bound(&search_key);
                    let swip = internal.edge_at(pos)?;

                    (swip, pos)
                }
                Node::Leaf(ref _leaf) => {
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

            if c_swip.load(Ordering::Acquire, eg).as_raw() == needle.latch() as *const _ {
                break (c_swip, pos);
            }

            let guard = GenericBPlusTree::lock_coupling(&target_guard, c_swip, eg)?;
            p_guard = Some(target_guard);
            target_guard = guard;

            if let Some(tree_guard) = t_guard.take() {
                tree_guard.recheck()?;
            }
        };


        if c_swip.load(Ordering::Acquire, eg).as_raw() == needle.latch() as *const _ {
            target_guard.recheck()?;
            return Ok(ParentHandler::Parent { parent_guard: target_guard, pos });
        } else {
            return Err(error::Error::Unwind)
        }
    }

    pub(crate) fn find_nearest_leaf<'t, 'g, 'e>(&'t self, needle: &OptimisticGuard<'g, Node<K, V, IC, LC>>, direction: Direction, eg: &'e epoch::Guard) -> error::Result<Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, (OptimisticGuard<'e, Node<K, V, IC, LC>>, u16))>>
    where
        K: Ord
    {
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = unsafe { tree_guard.load(Ordering::Acquire, eg).deref() };
        let root_latch_ptr = root_latch as *const _;
        let root_guard = root_latch.optimistic_or_spin();

        if needle.latch() as *const _ == root_latch_ptr {
            root_guard.recheck()?;
            tree_guard.recheck()?;
            return error::Result::Ok(None);
        }


        let (parent_guard, pos) = match self.find_parent(needle, eg)? {
            ParentHandler::Root { tree_guard: _ } => {
                return error::Result::Ok(None);
            }
            ParentHandler::Parent { parent_guard, pos } => (parent_guard, pos)
        };

        let within_bounds = match direction {
            Direction::Forward => {
                pos + 1 <= parent_guard.as_internal().len
            }
            Direction::Reverse => {
                pos > 0
            }
        };

        if within_bounds {
            let lookup_pos = match direction {
                Direction::Forward => pos + 1,
                Direction::Reverse => pos - 1
            };
            let swip = parent_guard.as_internal().edge_at(lookup_pos)?;

            let guard = GenericBPlusTree::lock_coupling(&parent_guard, swip, eg)?;

            if guard.is_leaf() {
                guard.recheck()?;
                return error::Result::Ok(Some((guard, (parent_guard, lookup_pos))));
            } else {
                let (leaf, parent_opt) = self.find_leaf_and_parent_from_node(guard, direction, eg)?;
                return error::Result::Ok(Some((leaf, parent_opt.expect("must have parent here"))));
            }
        } else {
            let mut target_guard = parent_guard;

            loop {
                let (parent_guard, pos) = match self.find_parent(&target_guard, eg)? {
                    ParentHandler::Root { tree_guard: _ } => {
                        return error::Result::Ok(None);
                    }
                    ParentHandler::Parent { parent_guard, pos } => (parent_guard, pos)
                };

                let within_bounds = match direction {
                    Direction::Forward => {
                        pos + 1 <= parent_guard.as_internal().len
                    }
                    Direction::Reverse => {
                        pos > 0
                    }
                };

                if within_bounds {
                    let lookup_pos = match direction {
                        Direction::Forward => pos + 1,
                        Direction::Reverse => pos - 1
                    };
                    let swip = parent_guard.as_internal().edge_at(lookup_pos)?;

                    let guard = GenericBPlusTree::lock_coupling(&parent_guard, swip, eg)?;

                    if guard.is_leaf() {
                        guard.recheck()?;
                        return error::Result::Ok(Some((guard, (parent_guard, lookup_pos))));
                    } else {
                        let (leaf, parent_opt) = self.find_leaf_and_parent_from_node(guard, direction, eg)?;
                        return error::Result::Ok(Some((leaf, parent_opt.expect("must have parent here"))));
                    }
                } else {
                    target_guard = parent_guard;
                    continue;
                }
            }
        }
    }

    fn lock_coupling<'e>(p_guard: &OptimisticGuard<'e, Node<K, V, IC, LC>>, swip: &Atomic<HybridLatch<Node<K, V, IC, LC>>>, eg: &'e epoch::Guard) -> error::Result<OptimisticGuard<'e, Node<K, V, IC, LC>>> {
        let latch = unsafe { swip.load(Ordering::Acquire, eg).deref() };
        let guard = latch.optimistic_or_spin();

        p_guard.recheck()?;

        Ok(guard)
    }

    fn lock_coupling_shared<'e>(p_guard: &OptimisticGuard<'e, Node<K, V, IC, LC>>, swip: &Atomic<HybridLatch<Node<K, V, IC, LC>>>, eg: &'e epoch::Guard) -> error::Result<SharedGuard<'e, Node<K, V, IC, LC>>> {
        let latch = unsafe { swip.load(Ordering::Acquire, eg).deref() };
        let guard = latch.shared();

        p_guard.recheck()?;

        Ok(guard)
    }

    fn lock_coupling_exclusive<'e>(p_guard: &OptimisticGuard<'e, Node<K, V, IC, LC>>, swip: &Atomic<HybridLatch<Node<K, V, IC, LC>>>, eg: &'e epoch::Guard) -> error::Result<ExclusiveGuard<'e, Node<K, V, IC, LC>>> {
        let latch = unsafe { swip.load(Ordering::Acquire, eg).deref() };
        let guard = latch.exclusive();

        p_guard.recheck()?;

        Ok(guard)
    }

    fn find_leaf_and_parent_from_node<'t, 'g ,'e>(&'t self, needle: OptimisticGuard<'e, Node<K, V, IC, LC>>, direction: Direction, eg: &'e epoch::Guard) -> error::Result<(OptimisticGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>)> {
        let mut p_guard = None;
        let mut target_guard = needle;

        let leaf_guard = loop {
            let (c_swip, pos) = match *target_guard {
                Node::Internal(ref internal) => {
                    let pos = match direction {
                        Direction::Forward => 0,
                        Direction::Reverse => internal.len
                    };
                    let swip = internal.edge_at(pos)?;
                    (swip, pos)
                }
                Node::Leaf(ref _leaf) => {
                    break target_guard;
                }
            };

            let guard = GenericBPlusTree::lock_coupling(&target_guard, c_swip, eg)?;
            p_guard = Some((target_guard, pos));
            target_guard = guard;
        };

        leaf_guard.recheck()?;

        Ok((leaf_guard, p_guard))
    }

    fn find_first_leaf_and_parent<'t, 'e>(&'t self, eg: &'e epoch::Guard) -> error::Result<(OptimisticGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>)> {
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = unsafe { tree_guard.load(Ordering::Acquire, eg).deref() };
        let root_guard = root_latch.optimistic_or_spin();
        tree_guard.recheck()?;

        self.find_leaf_and_parent_from_node(root_guard, Direction::Forward, eg)
    }

    fn find_last_leaf_and_parent<'t, 'e>(&'t self, eg: &'e epoch::Guard) -> error::Result<(OptimisticGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>)> {
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = unsafe { tree_guard.load(Ordering::Acquire, eg).deref() };
        let root_guard = root_latch.optimistic_or_spin();
        tree_guard.recheck()?;

        self.find_leaf_and_parent_from_node(root_guard, Direction::Reverse, eg)
    }

    fn find_leaf_and_parent<'t, 'k ,'e, Q>(&'t self, key: &'k Q, eg: &'e epoch::Guard) -> error::Result<(OptimisticGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>)>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = unsafe { tree_guard.load(Ordering::Acquire, eg).deref() };
        let root_guard = root_latch.optimistic_or_spin();
        tree_guard.recheck()?;

        let mut t_guard = Some(tree_guard);
        let mut p_guard = None;
        let mut target_guard = root_guard;

        // let mut level = 0u16;

        let leaf_guard = loop {
            let (c_swip, pos) = match *target_guard {
                Node::Internal(ref internal) => {
                    let (pos, _) = internal.lower_bound(key);
                    let swip = internal.edge_at(pos)?;
                    (swip, pos)
                }
                Node::Leaf(ref _leaf) => {
                    break target_guard;
                }
            };

            // TODO if level == height - 1 use shared mode

            let guard = GenericBPlusTree::lock_coupling(&target_guard, c_swip, eg)?;
            p_guard = Some((target_guard, pos));
            target_guard = guard;

            if let Some(tree_guard) = t_guard.take() {
                tree_guard.recheck()?;
            }

            // level += 1;
        };

        Ok((leaf_guard, p_guard))
    }

    fn find_leaf<'t, 'k ,'e, Q>(&'t self, key: &'k Q, eg: &'e epoch::Guard) -> error::Result<OptimisticGuard<'e, Node<K, V, IC, LC>>>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (leaf, _) = self.find_leaf_and_parent(key, eg)?;
        Ok(leaf)
    }

    pub(crate) fn find_shared_leaf_and_optimistic_parent<'t, 'k ,'e, Q>(&'t self, key: &'k Q, eg: &'e epoch::Guard) -> (SharedGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        loop {
            let perform = || {
                let tree_guard = self.root.optimistic_or_spin();
                let root_latch = unsafe { tree_guard.load(Ordering::Acquire, eg).deref() };
                let root_guard = root_latch.optimistic_or_spin();
                tree_guard.recheck()?;

                let mut t_guard = Some(tree_guard);
                let mut p_guard = None;
                let mut target_guard = root_guard;

                let mut level = 1u16;

                let leaf_guard = loop {
                    let (c_swip, pos) = match *target_guard {
                        Node::Internal(ref internal) => {
                            let (pos, _) = internal.lower_bound(key);
                            let swip = internal.edge_at(pos)?;
                            (swip, pos)
                        }
                        Node::Leaf(ref _leaf) => {
                            if let Some(tree_guard) = t_guard.take() {
                                tree_guard.recheck()?;
                            }

                            if p_guard.is_none() {
                                tp!("got root");
                                break target_guard.to_shared()?;
                            } else {
                                panic!("got a leaf on the wrong level");
                                // return Err(error::Error::Unwind);
                            }
                        }
                    };

                    // TODO if level == height - 1 use shared mode
                    if (level + 1) as usize == self.height.load(Ordering::Acquire) {
                        if let Some(tree_guard) = t_guard.take() {
                            tree_guard.recheck()?;
                        }

                        let guard = GenericBPlusTree::lock_coupling_shared(&target_guard, c_swip, eg)?;
                        p_guard = Some((target_guard, pos));

                        break guard;
                    } else {
                        let guard = GenericBPlusTree::lock_coupling(&target_guard, c_swip, eg)?;
                        p_guard = Some((target_guard, pos));
                        target_guard = guard;

                        if let Some(tree_guard) = t_guard.take() {
                            tree_guard.recheck()?;
                        }

                        level += 1;
                    }
                };

                error::Result::Ok((leaf_guard, p_guard))
            };

            match perform() {
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

    pub(crate) fn find_first_shared_leaf_and_optimistic_parent<'t, 'e>(&'t self, eg: &'e epoch::Guard) -> (SharedGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>) {
        loop {
            let perform = || {
                let (leaf, parent_opt) = self.find_first_leaf_and_parent(eg)?;
                let shared_leaf = leaf.to_shared()?;
                error::Result::Ok((shared_leaf, parent_opt))
            };

            match perform() {
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

    pub(crate) fn find_last_shared_leaf_and_optimistic_parent<'t, 'e>(&'t self, eg: &'e epoch::Guard) -> (SharedGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>) {
        loop {
            let perform = || {
                let (leaf, parent_opt) = self.find_last_leaf_and_parent(eg)?;
                let shared_leaf = leaf.to_shared()?;
                error::Result::Ok((shared_leaf, parent_opt))
            };

            match perform() {
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

    pub(crate) fn find_exclusive_leaf_and_optimistic_parent<'t, 'k ,'e, Q>(&'t self, key: &'k Q, eg: &'e epoch::Guard) -> (ExclusiveGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        loop {
            let perform = || {
                let tree_guard = self.root.optimistic_or_spin();
                let root_latch = unsafe { tree_guard.load(Ordering::Acquire, eg).deref() };
                let root_guard = root_latch.optimistic_or_spin();
                tree_guard.recheck()?;

                let mut t_guard = Some(tree_guard);
                let mut p_guard = None;
                let mut target_guard = root_guard;

                let mut level = 1u16;

                let leaf_guard = loop {
                    let (c_swip, pos) = match *target_guard {
                        Node::Internal(ref internal) => {
                            let (pos, _) = internal.lower_bound(key);
                            let swip = internal.edge_at(pos)?;
                            (swip, pos)
                        }
                        Node::Leaf(ref _leaf) => {
                            if let Some(tree_guard) = t_guard.take() {
                                tree_guard.recheck()?;
                            }

                            if p_guard.is_none() {
                                break target_guard.to_exclusive()?;
                            } else {
                                panic!("got a leaf on the wrong level");
                                // return Err(error::Error::Unwind);
                            }
                        }
                    };

                    // TODO if level == height - 1 use shared mode
                    if (level + 1) as usize == self.height.load(Ordering::Acquire) {
                        if let Some(tree_guard) = t_guard.take() {
                            tree_guard.recheck()?;
                        }

                        let guard = GenericBPlusTree::lock_coupling_exclusive(&target_guard, c_swip, eg)?;
                        p_guard = Some((target_guard, pos));

                        break guard;
                    } else {
                        let guard = GenericBPlusTree::lock_coupling(&target_guard, c_swip, eg)?;
                        p_guard = Some((target_guard, pos));
                        target_guard = guard;

                        if let Some(tree_guard) = t_guard.take() {
                            tree_guard.recheck()?;
                        }

                        level += 1;
                    }
                };

                error::Result::Ok((leaf_guard, p_guard))
            };

            match perform() {
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

    pub(crate) fn find_exact_exclusive_leaf_and_optimistic_parent<'t, 'k ,'e, Q>(&'t self, key: &'k Q, eg: &'e epoch::Guard) -> Option<((ExclusiveGuard<'e, Node<K, V, IC, LC>>, u16), Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>)>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        loop {
            let perform = || {
                let (leaf, parent_opt) = self.find_leaf_and_parent(key, eg)?;
                let (pos, exact) = leaf.as_leaf().lower_bound(key);
                if exact {
                    let exclusive_leaf = leaf.to_exclusive()?;
                    error::Result::Ok(Some(((exclusive_leaf, pos), parent_opt)))
                } else {
                    leaf.recheck()?;
                    error::Result::Ok(None)
                }
            };

            match perform() {
                Ok(opt) => {
                    return opt;
                }
                Err(_) => {
                    // TODO backoff
                    continue;
                }
            }
        }
    }

    pub(crate) fn find_first_exclusive_leaf_and_optimistic_parent<'t, 'e>(&'t self, eg: &'e epoch::Guard) -> (ExclusiveGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>) {
        loop {
            let perform = || {
                let (leaf, parent_opt) = self.find_first_leaf_and_parent(eg)?;
                let exclusive_leaf = leaf.to_exclusive()?;
                error::Result::Ok((exclusive_leaf, parent_opt))
            };

            match perform() {
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

    pub(crate) fn find_last_exclusive_leaf_and_optimistic_parent<'t, 'e>(&'t self, eg: &'e epoch::Guard) -> (ExclusiveGuard<'e, Node<K, V, IC, LC>>, Option<(OptimisticGuard<'e, Node<K, V, IC, LC>>, u16)>) {
        loop {
            let perform = || {
                let (leaf, parent_opt) = self.find_last_leaf_and_parent(eg)?;
                let exclusive_leaf = leaf.to_exclusive()?;
                error::Result::Ok((exclusive_leaf, parent_opt))
            };

            match perform() {
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

    pub fn lookup<Q, R, F>(&self, key: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord,
        F: Fn(&V) -> R
    {
        let eg = &epoch::pin();
        loop {
            let perform = || {
                let guard = self.find_leaf(key, eg)?;
                if let Node::Leaf(ref leaf) = *guard {
                    let (pos, exact) = leaf.lower_bound(key);
                    if exact {
                        let result = f(leaf.value_at(pos)?);
                        guard.recheck()?;
                        error::Result::Ok(Some(result))
                    } else {
                        guard.recheck()?;
                        error::Result::Ok(None)
                    }
                } else {
                    unreachable!("must be a leaf node");
                }
            };

            match perform() {
                Ok(opt) => {
                    return opt;
                }
                Err(_) => {
                    // TODO backoff
                    continue;
                }
            }
        }
    }

    pub fn remove<'t, 'k, Q>(&'t self, key: &'k Q) -> Option<(K, V)>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let eg = &epoch::pin();

        let opt = if let Some(((mut guard, pos), _parent_opt)) = self.find_exact_exclusive_leaf_and_optimistic_parent(key, eg) {
            let kv = guard.as_leaf_mut().remove_at(pos);

            if guard.is_underfull() {
                let guard = guard.unlock();
                loop {
                    let perform_merge = || {
                        let _ = self.try_merge(&guard, eg)?;
                        error::Result::Ok(())
                    };

                    match perform_merge() {
                        Ok(_) => {
                            break;
                        },
                        Err(error::Error::Reclaimed) => {
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

            Some(kv)
        } else {
            None
        };

        return opt;
    }

    pub(crate) fn try_split<'t, 'g, 'e>(&'t self, needle: &OptimisticGuard<'g, Node<K, V, IC, LC>>, eg: &'e epoch::Guard) -> error::Result<()>
    where
        K: Ord
    {
        let parent_handler = self.find_parent(needle, eg)?;

        match parent_handler {
            ParentHandler::Root { tree_guard } => {
                let mut tree_guard_x = tree_guard.to_exclusive()?; // TODO tree root should not need this lock (use atomic store)

                let root_latch = unsafe { tree_guard_x.load(Ordering::Acquire, eg).deref() };
                let mut root_guard_x = root_latch.exclusive();

                // TODO assert!(height == 1 || !root_guard_x.is_leaf());

                let mut new_root_owned: Owned<HybridLatch<Node<K, V, IC, LC>>> = Owned::new(
                    HybridLatch::new(Node::Internal(InternalNode::new()))
                );

                match root_guard_x.as_mut() {
                    Node::Internal(root_internal_node) => {
                        if root_internal_node.len <= 2 {
                            return Ok(())
                        }

                        let split_pos = root_internal_node.len / 2; // TODO choose a better split position if bulk loading
                        let split_key = root_internal_node.key_at(split_pos).expect("should exist").clone();

                        let mut new_right_node_owned = Owned::new(
                            HybridLatch::new(Node::Internal(InternalNode::new()))
                        );

                        {
                            let new_right_node = new_right_node_owned.as_mut().as_mut().as_internal_mut();
                            root_internal_node.split(new_right_node, split_pos);
                        }

                        let old_root_edge = Atomic::from(tree_guard_x.load(Ordering::Acquire, eg));
                        let new_right_node_edge = Atomic::<HybridLatch<Node<K, V, IC, LC>>>::from(new_right_node_owned);

                        {
                            let new_root = new_root_owned.as_mut().as_mut().as_internal_mut();
                            new_root.insert(split_key, old_root_edge).expect("must have space");
                            new_root.upper_edge = Some(new_right_node_edge);
                        }
                    }
                    Node::Leaf(root_leaf_node) => {
                        if root_leaf_node.len <= 2 {
                            return Ok(())
                        }

                        let split_pos = root_leaf_node.len / 2; // TODO choose a better split position if bulk loading
                        let split_key = root_leaf_node.key_at(split_pos).expect("should exist").clone();

                        let mut new_right_node_owned = Owned::new(
                            HybridLatch::new(Node::Leaf(LeafNode::new()))
                        );

                        {
                            let new_right_node = new_right_node_owned.as_mut().as_mut().as_leaf_mut();
                            root_leaf_node.split(new_right_node, split_pos);
                        }

                        let old_root_edge = Atomic::from(tree_guard_x.load(Ordering::Acquire, eg));
                        let new_right_node_edge = Atomic::<HybridLatch<Node<K, V, IC, LC>>>::from(new_right_node_owned);

                        {
                            let new_root = new_root_owned.as_mut().as_mut().as_internal_mut();
                            new_root.insert(split_key, old_root_edge).expect("must have space");
                            new_root.upper_edge = Some(new_right_node_edge);
                        }
                    }
                }

                let new_root_node_edge = Atomic::<HybridLatch<Node<K, V, IC, LC>>>::from(new_root_owned);
                *tree_guard_x = new_root_node_edge;
                self.height.fetch_add(1, Ordering::Relaxed);
            },
            ParentHandler::Parent { parent_guard, pos } => {
                if parent_guard.as_internal().has_space() {
                    let swip = parent_guard.as_internal().edge_at(pos)?;
                    let target_guard = GenericBPlusTree::lock_coupling(&parent_guard, swip, eg)?;
                    let mut parent_guard_x = parent_guard.to_exclusive()?;
                    let mut target_guard_x = target_guard.to_exclusive()?;

                    match target_guard_x.as_mut() {
                        Node::Internal(left_internal) => {
                            if left_internal.len <= 2 {
                                return Ok(())
                            }

                            let split_pos = left_internal.len / 2; // TODO choose a better split position if bulk loading
                            let split_key = left_internal.key_at(split_pos).expect("should exist").clone();

                            let mut new_right_node_owned = Owned::new(
                                HybridLatch::new(Node::Internal(InternalNode::new()))
                            );

                            {
                                let new_right_node = new_right_node_owned.as_mut().as_mut().as_internal_mut();
                                left_internal.split(new_right_node, split_pos);
                            }

                            let new_right_node_edge = Atomic::<HybridLatch<Node<K, V, IC, LC>>>::from(new_right_node_owned);

                            let parent_internal = parent_guard_x.as_internal_mut();

                            if pos == parent_internal.len {
                                let left_edge = parent_internal.upper_edge
                                    .replace(new_right_node_edge)
                                    .expect("upper_edge must be populated");
                                parent_internal.insert(split_key, left_edge);
                            } else {
                                let left_edge = std::mem::replace(&mut parent_internal.edges[pos as usize], new_right_node_edge);
                                parent_internal.insert(split_key, left_edge);
                            }
                        }
                        Node::Leaf(left_leaf) => {
                            if left_leaf.len <= 2 {
                                return Ok(())
                            }

                            let split_pos = left_leaf.len / 2; // TODO choose a better split position if bulk loading
                            let split_key = left_leaf.key_at(split_pos).expect("should exist").clone();

                            let mut new_right_node_owned = Owned::new(
                                HybridLatch::new(Node::Leaf(LeafNode::new()))
                            );

                            {
                                let new_right_node = new_right_node_owned.as_mut().as_mut().as_leaf_mut();
                                left_leaf.split(new_right_node, split_pos);
                            }

                            let new_right_node_edge = Atomic::<HybridLatch<Node<K, V, IC, LC>>>::from(new_right_node_owned);

                            let parent_internal = parent_guard_x.as_internal_mut();

                            if pos == parent_internal.len {
                                let left_edge = parent_internal.upper_edge
                                    .replace(new_right_node_edge)
                                    .expect("upper_edge must be populated");
                                parent_internal.insert(split_key, left_edge);
                            } else {
                                let left_edge = std::mem::replace(&mut parent_internal.edges[pos as usize], new_right_node_edge);
                                parent_internal.insert(split_key, left_edge);
                            }
                        }
                    }
                } else {
                    self.try_split(&parent_guard, eg)?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn try_merge<'t, 'g, 'e>(&'t self, needle: &OptimisticGuard<'g, Node<K, V, IC, LC>>, eg: &'e epoch::Guard) -> error::Result<bool>
    where
        K: Ord
    {
        let parent_handler = self.find_parent(needle, eg)?;

        match parent_handler {
            ParentHandler::Root { tree_guard: _ } => {
                return Ok(false);
            },
            ParentHandler::Parent { mut parent_guard, pos } => {
                let parent_len = parent_guard.as_internal().len;

                let swip = parent_guard.as_internal().edge_at(pos)?;
                let mut target_guard = GenericBPlusTree::lock_coupling(&parent_guard, swip, eg)?;

                if !target_guard.is_underfull() {
                    target_guard.recheck()?;
                    return Ok(false);
                }

                let merge_succeded = if parent_len > 1 && pos > 0 {
                    // Try merge left
                    let l_swip = parent_guard.as_internal().edge_at(pos - 1)?;
                    let left_guard = GenericBPlusTree::lock_coupling(&parent_guard, l_swip, eg)?;

                    if !(left_guard.can_merge_with(&target_guard)) {
                        left_guard.recheck()?;
                        target_guard.recheck()?;
                        false
                    } else {
                        let mut parent_guard_x = parent_guard.to_exclusive()?;
                        let mut target_guard_x = target_guard.to_exclusive()?;
                        let mut left_guard_x = left_guard.to_exclusive()?;

                        match target_guard_x.as_mut() {
                            Node::Leaf(ref mut target_leaf) => {
                                assert!(left_guard_x.is_leaf());

                                if !left_guard_x.as_leaf_mut().merge(target_leaf) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos == parent_len {
                                        let (_, left_edge) = parent_internal.remove_at(pos - 1);
                                        let dropped_edge = parent_internal
                                            .upper_edge
                                            .replace(left_edge).expect("must exist");

                                        let shared = dropped_edge.load(Ordering::Relaxed, eg);
                                        if !shared.is_null() {
                                            unsafe { eg.defer_destroy(shared) };
                                        }
                                    } else {
                                        let (_, left_edge) = parent_internal
                                            .remove_at(pos - 1);
                                        let dropped_edge = std::mem::replace(&mut parent_internal.edges[(pos - 1) as usize], left_edge);

                                        let shared = dropped_edge.load(Ordering::Relaxed, eg);
                                        if !shared.is_null() {
                                            unsafe { eg.defer_destroy(shared) };
                                        }
                                    }

                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
                                    true
                                }
                            }
                            Node::Internal(target_internal) => {
                                assert!(!left_guard_x.is_leaf());

                                if !left_guard_x.as_internal_mut().merge(target_internal) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos == parent_len {
                                        let (_, left_edge) = parent_internal.remove_at(pos - 1);
                                        let dropped_edge = parent_internal
                                            .upper_edge
                                            .replace(left_edge).expect("must exist");

                                        let shared = dropped_edge.load(Ordering::Relaxed, eg);
                                        if !shared.is_null() {
                                            unsafe { eg.defer_destroy(shared) };
                                        }
                                    } else {
                                        let (_, left_edge) = parent_internal
                                            .remove_at(pos - 1);
                                        let dropped_edge = std::mem::replace(&mut parent_internal.edges[(pos - 1) as usize], left_edge);

                                        let shared = dropped_edge.load(Ordering::Relaxed, eg);
                                        if !shared.is_null() {
                                            unsafe { eg.defer_destroy(shared) };
                                        }
                                    }

                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
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
                    let r_swip = parent_guard.as_internal().edge_at(pos + 1)?;
                    let right_guard = GenericBPlusTree::lock_coupling(&parent_guard, r_swip, eg)?;

                    if !(right_guard.can_merge_with(&target_guard)) {
                        right_guard.recheck()?;
                        target_guard.recheck()?;
                        false
                    } else {
                        let mut parent_guard_x = parent_guard.to_exclusive()?;
                        let mut target_guard_x = target_guard.to_exclusive()?;
                        let mut right_guard_x = right_guard.to_exclusive()?;

                        match target_guard_x.as_mut() {
                            Node::Leaf(ref mut target_leaf) => {
                                assert!(right_guard_x.is_leaf());

                                if !target_leaf.merge(right_guard_x.as_leaf_mut()) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos + 1 == parent_len {
                                        let (_, left_edge) = parent_internal.remove_at(pos);
                                        let dropped_edge = parent_internal
                                            .upper_edge
                                            .replace(left_edge).expect("must exist");

                                        let shared = dropped_edge.load(Ordering::Relaxed, eg);
                                        if !shared.is_null() {
                                            unsafe { eg.defer_destroy(shared) };
                                        }
                                    } else {
                                        let (_, left_edge) = parent_internal
                                            .remove_at(pos);
                                        let dropped_edge = std::mem::replace(&mut parent_internal.edges[pos as usize], left_edge);

                                        let shared = dropped_edge.load(Ordering::Relaxed, eg);
                                        if !shared.is_null() {
                                            unsafe { eg.defer_destroy(shared) };
                                        }
                                    }

                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    true
                                }
                            }
                            Node::Internal(target_internal) => {
                                assert!(!right_guard_x.is_leaf());

                                if !target_internal.merge(right_guard_x.as_internal_mut()) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos + 1 == parent_len {
                                        let (_, left_edge) = parent_internal.remove_at(pos);
                                        let dropped_edge = parent_internal
                                            .upper_edge
                                            .replace(left_edge).expect("must exist");

                                        let shared = dropped_edge.load(Ordering::Relaxed, eg);
                                        if !shared.is_null() {
                                            unsafe { eg.defer_destroy(shared) };
                                        }
                                    } else {
                                        let (_, left_edge) = parent_internal
                                            .remove_at(pos);
                                        let dropped_edge = std::mem::replace(&mut parent_internal.edges[pos as usize], left_edge);

                                        let shared = dropped_edge.load(Ordering::Relaxed, eg);
                                        if !shared.is_null() {
                                            unsafe { eg.defer_destroy(shared) };
                                        }
                                    }

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
                        let _ = self.try_merge(&parent_guard, eg)?;
                    }
                    error::Result::Ok(())
                };

                let _ = parent_merge();

                return Ok(merge_succeded)
            }
        }
    }

    pub fn raw_iter<'t>(&'t self) -> iter::RawSharedIter<'t, K, V, IC, LC>
    where
        K: Ord
    {
        iter::RawSharedIter::new(self)
    }

    pub fn raw_iter_mut<'t>(&'t self) -> iter::RawExclusiveIter<'t, K, V, IC, LC>
    where
        K: Ord
    {
        iter::RawExclusiveIter::new(self)
    }

    pub fn len(&self) -> usize {
        let mut count = 0usize;
        let mut iter = self.raw_iter();
        while let Some(_) = iter.next() {
            count += 1;
        }
        count
    }
}

pub(crate) enum Node<K, V, const IC: usize, const LC: usize> {
    Internal(InternalNode<K, V, IC, LC>),
    Leaf(LeafNode<K, V, LC>)
}

impl<K: fmt::Debug, V: fmt::Debug, const IC: usize, const LC: usize> fmt::Debug for Node<K, V, IC, LC> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Node::Internal(ref internal) => {
                f.debug_tuple("Internal")
                    .field(internal)
                    .finish()
            }
            &Node::Leaf(ref leaf) => {
                f.debug_tuple("Leaf")
                    .field(leaf)
                    .finish()
            }
        }
    }
}

impl<K, V, const IC: usize, const LC: usize> Node<K, V, IC, LC> {
    #[inline]
    pub(crate) fn is_leaf(&self) -> bool {
        match self {
            Node::Leaf(_) => true,
            Node::Internal(_) => false
        }
    }

    #[inline]
    pub(crate) fn as_leaf(&self) -> &LeafNode<K, V, LC> {
        match self {
            Node::Leaf(ref leaf) => leaf,
            Node::Internal(_) => {
                panic!("expected leaf node");
            }
        }
    }

    #[inline]
    pub(crate) fn as_leaf_mut(&mut self) -> &mut LeafNode<K, V, LC> {
        match self {
            Node::Leaf(ref mut leaf) => leaf,
            Node::Internal(_) => {
                panic!("expected leaf node");
            }
        }
    }

    #[inline]
    pub(crate) fn as_internal(&self) -> &InternalNode<K, V, IC, LC> {
        match self {
            Node::Internal(ref internal) => internal,
            Node::Leaf(_) => {
                panic!("expected internal node");
            }
        }
    }

    #[inline]
    pub(crate) fn as_internal_mut(&mut self) -> &mut InternalNode<K, V, IC, LC> {
        match self {
            Node::Internal(ref mut internal) => internal,
            Node::Leaf(_) => {
                panic!("expected internal node");
            }
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn keys(&self) -> &[K] {
        match self {
            Node::Internal(ref internal) => &internal.keys,
            Node::Leaf(ref leaf) => &leaf.keys
        }
    }

    #[inline]
    pub(crate) fn sample_key(&self) -> Option<&K> {
        match self {
            Node::Internal(ref internal) => internal.sample_key.as_ref(),
            Node::Leaf(ref leaf) => leaf.sample_key.as_ref()
        }
    }

    #[inline]
    pub(crate) fn is_underfull(&self) -> bool {
        match self {
            Node::Internal(ref internal) => internal.is_underfull(),
            Node::Leaf(ref leaf) => leaf.is_underfull()
        }
    }

    #[inline]
    pub(crate) fn can_merge_with(&self, other: &Self) -> bool {
        match self {
            Node::Internal(ref internal) => {
                match other {
                    Node::Internal(ref other) => {
                        ((internal.len + 1 + other.len) as usize) < IC /* INNER_CAPACITY */
                    }
                    _ => false
                }
            },
            Node::Leaf(ref leaf) => {
                match other {
                    Node::Leaf(ref other) => {
                        ((leaf.len + other.len) as usize) < LC /* LEAF_CAPACITY */
                    }
                    _ => false
                }
            }
        }
    }
}

pub(crate) struct LeafNode<K, V, const LEAF_CAPACITY: usize> {
    len: u16,
    keys: SmallVec<[K; LEAF_CAPACITY]>,
    values: SmallVec<[V; LEAF_CAPACITY]>,
    lower_fence: Option<K>,
    upper_fence: Option<K>,
    sample_key: Option<K> // All but the root must keep a sample key
}

impl<K: fmt::Debug, V: fmt::Debug, const LEAF_CAPACITY: usize> fmt::Debug for LeafNode<K, V, LEAF_CAPACITY> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeafNode")
         .field("len", &self.len)
         .field("keys", &self.keys)
         .field("values", &self.values)
         .field("lower_fence", &self.lower_fence)
         .field("upper_fence", &self.upper_fence)
         .field("sample_key", &self.sample_key)
         .finish()
    }
}

impl<K, V, const LEAF_CAPACITY: usize> LeafNode<K, V, LEAF_CAPACITY> {
    pub fn new() -> LeafNode<K, V, LEAF_CAPACITY> {
        LeafNode {
            len: 0,
            keys: smallvec![],
            values: smallvec![],
            lower_fence: None,
            upper_fence: None,
            sample_key: None
        }
    }

    #[inline]
    pub(crate) fn lower_bound<Q>(&self, key: &Q) -> (u16, bool)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        if self.lower_fence().map(|fk| key < fk.borrow()).unwrap_or(false) {
            return (0, false);
        }

        match self.upper_fence() {
            Some(fk) => {
                if key > fk.borrow() {
                    return (self.len, false);
                }
            }
            None => {}
        }

        let mut lower = 0;
        let mut upper = self.len;

        while lower < upper {
            let mid = ((upper - lower) / 2) + lower;

            if key < unsafe { self.keys.get_unchecked(mid as usize) }.borrow() {
                upper = mid;
            } else if key > unsafe { self.keys.get_unchecked(mid as usize) }.borrow() {
                lower = mid + 1;
            } else {
                return (mid, true);
            }
        }

        (lower, false)
    }

    #[inline]
    pub(crate) fn lower_fence(&self) -> Option<&K> {
        self.lower_fence.as_ref()
    }

    #[inline]
    pub(crate) fn upper_fence(&self) -> Option<&K> {
        self.upper_fence.as_ref()
    }

    #[inline]
    pub(crate) fn value_at(&self, pos: u16) -> error::Result<&V> {
        Ok(unsafe { self.values.get_unchecked(pos as usize) })
    }

    #[inline]
    pub(crate) fn key_at(&self, pos: u16) -> error::Result<&K> {
        Ok(unsafe { self.keys.get_unchecked(pos as usize) })
    }

    #[inline]
    pub(crate) fn kv_at(&self, pos: u16) -> error::Result<(&K, &V)> {
        Ok((self.key_at(pos)?, self.value_at(pos)?))
    }

    #[inline]
    pub(crate) fn kv_at_mut(&mut self, pos: u16) -> error::Result<(&K, &mut V)> {
        Ok(unsafe { (self.keys.get_unchecked(pos as usize), self.values.get_unchecked_mut(pos as usize)) })
    }

    #[inline]
    pub(crate) fn has_space(&self) -> bool {
        (self.len as usize) < LEAF_CAPACITY
    }

    #[inline]
    pub(crate) fn is_underfull(&self) -> bool {
        (self.len as usize) < (LEAF_CAPACITY as f32 * 0.4) as usize
    }

    // OBS: Does not trigger splits
    #[cfg(test)]
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<u16>
    where
        K: Ord
    {
        let (pos, exact) = self.lower_bound(&key);
        if exact {
            unimplemented!("upserts");
        } else {
            if !self.has_space() {
                return None;
            }

            self.keys.insert(pos as usize, key);
            self.values.insert(pos as usize, value);
            self.len += 1;
        }
        Some(pos)
    }

    // OBS: Does not trigger splits
    pub(crate) fn insert_at(&mut self, pos: u16, key: K, value: V) -> Option<u16> {
        if !self.has_space() {
            return None;
        }

        self.keys.insert(pos as usize, key);
        self.values.insert(pos as usize, value);
        self.len += 1;

        Some(pos)
    }

    // OBS: Does not trigger merges
    #[cfg(test)]
    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (pos, exact) = self.lower_bound(key);
        if !exact {
            return None;
        } else {
            let _ = self.keys.remove(pos as usize);
            let value = self.values.remove(pos as usize); // TODO Can be improved by postponing compaction
            self.len -= 1;

            return Some(value);
        }
    }

    pub(crate) fn remove_at(&mut self, pos: u16) -> (K, V) {
        self.len -= 1;
        let key = self.keys.remove(pos as usize);
        let value = self.values.remove(pos as usize);

        (key, value)
    }

    #[inline]
    pub(crate) fn within_bounds<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        match (self.lower_fence().map(Borrow::borrow), self.upper_fence().map(Borrow::borrow)) {
            (Some(lf), Some(uf)) => key > lf && key <= uf,
            (Some(lf), None) => key > lf,
            (None, Some(uf)) => key <= uf,
            (None, None) => true
        }
    }
}

impl<K: Clone, V, const LEAF_CAPACITY: usize> LeafNode<K, V, LEAF_CAPACITY> {
    pub(crate) fn split(&mut self, right: &mut LeafNode<K, V, LEAF_CAPACITY>, split_pos: u16) {
        let split_key = self.key_at(split_pos).expect("should exist").clone();
        right.lower_fence = Some(split_key.clone());
        right.upper_fence = self.upper_fence.clone();

        self.upper_fence = Some(split_key);

        assert!(right.keys.len() == 0);
        assert!(right.values.len() == 0);
        right.keys.extend(self.keys.drain((split_pos + 1) as usize..));
        right.values.extend(self.values.drain((split_pos + 1) as usize..));

        self.sample_key = Some(self.keys[0].clone());
        right.sample_key = Some(right.keys[0].clone());

        right.len = right.keys.len() as u16;
        self.len = self.keys.len() as u16; // TODO move this to before drain
    }

    pub(crate) fn merge(&mut self, right: &mut LeafNode<K, V, LEAF_CAPACITY>) -> bool {
        if (self.len + right.len) as usize > LEAF_CAPACITY { // TODO better merge threshold
            return false;
        }

        right.len = 0;
        self.upper_fence = right.upper_fence.take();
        self.keys.extend(right.keys.drain(..));
        self.values.extend(right.values.drain(..));

        self.sample_key = right.sample_key.take();

        self.len = self.keys.len() as u16;
        true
    }
}

pub(crate) struct InternalNode<K, V, const INNER_CAPACITY: usize, const LC: usize> {
    len: u16,
    keys: SmallVec<[K; INNER_CAPACITY]>,
    edges: SmallVec<[Atomic<HybridLatch<Node<K, V, INNER_CAPACITY, LC>>>; INNER_CAPACITY]>,
    upper_edge: Option<Atomic<HybridLatch<Node<K, V, INNER_CAPACITY, LC>>>>,
    lower_fence: Option<K>,
    upper_fence: Option<K>,
    sample_key: Option<K> // All but the root must keep a sample key
}

impl<K: fmt::Debug, V: fmt::Debug, const INNER_CAPACITY: usize, const LC: usize> fmt::Debug for InternalNode<K, V, INNER_CAPACITY, LC> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InternalNode")
         .field("len", &self.len)
         .field("keys", &self.keys)
         .field("edges", &self.edges)
         .field("upper_edge", &self.upper_edge)
         .field("lower_fence", &self.lower_fence)
         .field("upper_fence", &self.upper_fence)
         .field("sample_key", &self.sample_key)
         .finish()
    }
}

impl<K, V, const INNER_CAPACITY: usize, const LC: usize> InternalNode<K, V, INNER_CAPACITY, LC> {
    pub(crate) fn new() -> InternalNode<K, V, INNER_CAPACITY, LC> {
        InternalNode {
            len: 0,
            keys: smallvec![],
            edges: smallvec![],
            upper_edge: None,
            lower_fence: None,
            upper_fence: None,
            sample_key: None
        }
    }

    #[inline]
    pub(crate) fn lower_bound<Q>(&self, key: &Q) -> (u16, bool)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        if self.lower_fence().map(|fk| key < fk.borrow()).unwrap_or(false) {
            return (0, false);
        }

        match self.upper_fence() {
            Some(fk) => {
                if key > fk.borrow() {
                    return (self.len, false);
                }
            }
            None => {}
        }

        let mut lower = 0;
        let mut upper = self.len;

        while lower < upper {
            let mid = ((upper - lower) / 2) + lower;

            if key < unsafe { self.keys.get_unchecked(mid as usize) }.borrow() {
                upper = mid;
            } else if key > unsafe { self.keys.get_unchecked(mid as usize) }.borrow() {
                lower = mid + 1;
            } else {
                return (mid, true);
            }
        }

        (lower, false)
    }

    #[inline]
    pub(crate) fn lower_fence(&self) -> Option<&K> {
        self.lower_fence.as_ref()
    }

    #[inline]
    pub(crate) fn upper_fence(&self) -> Option<&K> {
        self.upper_fence.as_ref()
    }

    #[inline]
    pub(crate) fn edge_at(&self, pos: u16) -> error::Result<&Atomic<HybridLatch<Node<K, V, INNER_CAPACITY, LC>>>> {
        if pos == self.len {
            if let Some(upper_edge) = self.upper_edge.as_ref() {
                Ok(upper_edge)
            } else {
                println!("unwind due to missing upper edge");
                Err(error::Error::Unwind)
            }
        } else {
            Ok(unsafe { self.edges.get_unchecked(pos as usize) })
        }
    }

    #[inline]
    pub(crate) fn key_at(&self, pos: u16) -> error::Result<&K> {
        Ok(unsafe { self.keys.get_unchecked(pos as usize) })
    }

    #[inline]
    pub(crate) fn has_space(&self) -> bool {
        (self.len as usize) < INNER_CAPACITY
    }

    #[inline]
    pub(crate) fn is_underfull(&self) -> bool {
        (self.len as usize) < (INNER_CAPACITY as f32 * 0.4) as usize
    }

    // OBS: Does not trigger splits nor updates upper_edge
    pub(crate) fn insert(&mut self, key: K, value: Atomic<HybridLatch<Node<K, V, INNER_CAPACITY, LC>>>) -> Option<u16>
    where
        K: Ord
    {
        let (pos, exact) = self.lower_bound(&key);
        if exact {
            unimplemented!("upserts");
        } else {
            if !self.has_space() {
                return None;
            }

            self.keys.insert(pos as usize, key);
            self.edges.insert(pos as usize, value);
            self.len += 1;
        }
        Some(pos)
    }

    // OBS: Does not trigger merges or reassings upper_edge
    #[cfg(test)]
    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<Atomic<HybridLatch<Node<K, V, INNER_CAPACITY, LC>>>>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (pos, exact) = self.lower_bound(key);
        if !exact {
            return None;
        } else {
            let _ = self.keys.remove(pos as usize);
            let edge = self.edges.remove(pos as usize); // TODO Can be improved by postponing compaction
            self.len -= 1;

            return Some(edge);
        }
    }

    pub(crate) fn remove_at(&mut self, pos: u16) -> (K, Atomic<HybridLatch<Node<K, V, INNER_CAPACITY, LC>>>) {
        let key = self.keys.remove(pos as usize);
        let edge = self.edges.remove(pos as usize);
        self.len -= 1;

        (key, edge)
    }
}

impl<K: Clone, V, const INNER_CAPACITY: usize, const LC: usize> InternalNode<K, V, INNER_CAPACITY, LC> {
    pub(crate) fn split(&mut self, right: &mut InternalNode<K, V, INNER_CAPACITY, LC>, split_pos: u16) {
        let split_key = self.key_at(split_pos).expect("should exist").clone();
        right.lower_fence = Some(split_key.clone());
        right.upper_fence = self.upper_fence.clone();

        self.upper_fence = Some(split_key);

        assert!(right.keys.len() == 0);
        assert!(right.edges.len() == 0);
        right.keys.extend(self.keys.drain((split_pos + 1) as usize..));
        right.edges.extend(self.edges.drain((split_pos + 1) as usize..));
        right.upper_edge = self.upper_edge.take();

        self.upper_edge = Some(self.edges.pop().unwrap());
        self.keys.pop().unwrap();

        self.sample_key = Some(self.keys[0].clone());
        right.sample_key = Some(right.keys[0].clone());

        right.len = right.keys.len() as u16;
        self.len = self.keys.len() as u16;
    }

    pub(crate) fn merge(&mut self, right: &mut InternalNode<K, V, INNER_CAPACITY, LC>) -> bool {
        if (self.len + right.len + 1 /* left upper edge */) as usize > INNER_CAPACITY {
            return false;
        }

        let _left_upper_fence = std::mem::replace(&mut self.upper_fence, right.upper_fence.take());
        let left_upper_edge = std::mem::replace(&mut self.upper_edge, right.upper_edge.take());

        self.keys.push(right.lower_fence.take().expect("right node must have lower fence"));
        self.edges.push(left_upper_edge.expect("left node must have upper edge"));

        self.keys.extend(right.keys.drain(..));
        self.edges.extend(right.edges.drain(..));

        self.sample_key = right.sample_key.take();

        self.len = self.keys.len() as u16;
        right.len = 0;

        true
    }
}

#[cfg(test)]
mod tests {
    use super::{GenericBPlusTree, Node, InternalNode, LeafNode, ParentHandler, Direction};
    use super::latch::HybridLatch;
    use smallvec::smallvec;
    use crossbeam_epoch::{self as epoch, Atomic};

    use serde::Deserialize;
    use std::sync::atomic::AtomicUsize;
    use super::util::sample_tree;

    #[test]
    fn sample_tree_works() {
        let bptree = sample_tree("fixtures/sample.json");

        let found = bptree.lookup("0003", |value| *value);
        assert_eq!(found, Some(3));

        let found = bptree.lookup("0005", |value| *value);
        assert_eq!(found, Some(5));

        let found = bptree.lookup("0004", |value| *value);
        assert_eq!(found, None);

        let found = bptree.lookup("0002", |value| *value);
        assert_eq!(found, Some(2));

        let found = bptree.lookup("0001", |value| *value);
        assert_eq!(found, None);
    }

    #[test]
    fn sample_tree_find_parent() {
        let bptree = sample_tree("fixtures/sample.json");

        let eg = &epoch::pin();

        let leaf = bptree.find_leaf("0002", eg).unwrap();
        assert_eq!(leaf.keys().first(), Some(&"0002".to_string()));

        if let ParentHandler::Parent { parent_guard, pos } = bptree.find_parent(&leaf, eg).unwrap() {
            assert_eq!(parent_guard.as_internal().keys.first(), Some(&"0002".to_string()));

            if let ParentHandler::Parent { parent_guard, pos } = bptree.find_parent(&parent_guard, eg).unwrap() {
                assert_eq!(parent_guard.as_internal().keys.first(), Some(&"0003".to_string()));

                if let ParentHandler::Root { tree_guard } = bptree.find_parent(&parent_guard, eg).unwrap() {
                    assert_eq!(&bptree.root as *const _, tree_guard.latch() as *const _);
                } else {
                    panic!("missing root");
                }
            } else {
                panic!("missing parent");
            }
        } else {
            panic!("missing parent");
        }

        let leaf = bptree.find_leaf("0005", eg).unwrap();
        assert_eq!(leaf.keys().first(), Some(&"0005".to_string()));

        if let ParentHandler::Parent { parent_guard, pos } = bptree.find_parent(&leaf, eg).unwrap() {
            assert_eq!(parent_guard.as_internal().keys.first(), Some(&"0005".to_string()));
        } else {
            panic!("missing parent");
        }
    }

    #[test]
    fn sample_tree_find_nearest_leaf() {
        let bptree = sample_tree("fixtures/sample.json");

        let eg = &epoch::pin();

        let leaf = bptree.find_leaf("0002", eg).unwrap();
        assert_eq!(leaf.keys().first(), Some(&"0002".to_string()));

        let (next_leaf, (parent, pos)) = bptree.find_nearest_leaf(&leaf, Direction::Forward, eg).unwrap().unwrap();
        assert!(next_leaf.is_leaf());
        assert_eq!(next_leaf.keys().first(), Some(&"0003".to_string()));
        assert_eq!(parent.keys().first(), Some(&"0002".to_string()));

        let (next_leaf, (parent, pos)) = bptree.find_nearest_leaf(&next_leaf, Direction::Forward, eg).unwrap().unwrap();
        assert!(next_leaf.is_leaf());
        assert_eq!(next_leaf.keys().first(), Some(&"0005".to_string()));
        assert_eq!(parent.keys().first(), Some(&"0005".to_string()));

        let (next_leaf, (parent, pos)) = bptree.find_nearest_leaf(&next_leaf, Direction::Reverse, eg).unwrap().unwrap();
        assert!(next_leaf.is_leaf());
        assert_eq!(next_leaf.keys().first(), Some(&"0003".to_string()));
        assert_eq!(parent.keys().first(), Some(&"0002".to_string()));

        let (next_leaf, (parent, pos)) = bptree.find_nearest_leaf(&next_leaf, Direction::Reverse, eg).unwrap().unwrap();
        assert!(next_leaf.is_leaf());
        assert_eq!(next_leaf.keys().first(), Some(&"0002".to_string()));
        assert_eq!(parent.keys().first(), Some(&"0002".to_string()));

        assert!(bptree.find_nearest_leaf(&next_leaf, Direction::Reverse, eg).unwrap().is_none());

        let (next_leaf, (parent, pos)) = bptree.find_nearest_leaf(&parent, Direction::Forward, eg).unwrap().unwrap();
        assert!(next_leaf.is_leaf());
        assert_eq!(next_leaf.keys().first(), Some(&"0005".to_string()));
        assert_eq!(parent.keys().first(), Some(&"0005".to_string()));
    }

    #[test]
    fn leaf_node_lower_bound() {
        let node: LeafNode<_, _, 16> = LeafNode {
            len: 3,
            keys: smallvec!["0001".to_string(), "0002".to_string(), "0004".to_string()],
            values: smallvec![1u64, 2, 4],
            lower_fence: None,
            upper_fence: None,
            sample_key: None
        };

        assert!(node.lower_bound("0001") == (0, true));
        assert!(node.lower_bound("0002") == (1, true));
        assert!(node.lower_bound("00002") == (0, false));
        assert!(node.lower_bound("0005") == (3, false));
        assert!(node.lower_bound("0003") == (2, false));
    }

    #[test]
    fn leaf_node_insert() {
        let mut node: LeafNode<_, _, 16> = LeafNode {
            len: 0,
            keys: smallvec![],
            values: smallvec![],
            lower_fence: None,
            upper_fence: None,
            sample_key: None
        };

        node.insert("0001", 1u64).unwrap();
        node.insert("0002", 2u64).unwrap();
        node.insert("0004", 4u64).unwrap();

        assert!(node.lower_bound("0001") == (0, true));
        assert!(node.lower_bound("0002") == (1, true));
        assert!(node.lower_bound("00002") == (0, false));
        assert!(node.lower_bound("0005") == (3, false));
        assert!(node.lower_bound("0003") == (2, false));

        node.remove("0001").unwrap();
        node.remove("0002").unwrap();
        node.remove("0004").unwrap();

        assert!(node.remove("0005").is_none());

        assert!(node.len == 0);

        assert!(node.lower_bound("0001") == (0, false));
    }
}
