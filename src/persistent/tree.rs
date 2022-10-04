use bplustree::latch::{HybridLatch, OptimisticGuard, SharedGuard, ExclusiveGuard, HybridGuard};
use bplustree::error::{self, NonOptimisticExt, BufMgrError};
use super::node::{Node, NodeKind, LeafNode, InternalNode};
use super::bufmgr::{
    registry::{ManagedDataStructure, ParentResult, DataStructureId},
    swip::Swip, OptSwipGuard, ShrSwipGuard, ExvSwipGuard, BufferFrame,
    BfState
};
use super::bufmgr;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering}
};

pub struct PersistentBPlusTree {
    root: HybridLatch<Swip<HybridLatch<BufferFrame>>>,
    height: AtomicUsize,
    dtid: DataStructureId
}

pub(crate) enum ParentHandler<'t> {
    Root { tree_guard: OptimisticGuard<'t, Swip<HybridLatch<BufferFrame>>> },
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

pub type OptNodeGuard = OptimisticGuard<'static, Node, BufferFrame>;
pub type ShrNodeGuard = SharedGuard<'static, Node, BufferFrame>;
pub type ExvNodeGuard = ExclusiveGuard<'static, Node, BufferFrame>;

impl ManagedDataStructure for PersistentBPlusTree {
    type PageValue = Node;
    fn bf_to_page_value(bf: &BufferFrame) -> &Self::PageValue {
        unsafe { & *(std::ptr::addr_of!(bf.page.value) as *const Node) }
    }
    fn bf_to_page_value_mut(bf: &mut BufferFrame) -> &mut Self::PageValue {
        unsafe { &mut *(std::ptr::addr_of_mut!(bf.page.value) as *mut Node) }
    }
    fn find_parent(&self, needle: &impl HybridGuard<Self::PageValue, BufferFrame>) -> error::Result<ParentResult> {
        let parent_handler = self.find_parent_or_unwind(needle)?;
        match parent_handler {
            ParentHandler::Root { tree_guard: _ } => Ok(ParentResult::Root),
            ParentHandler::Parent { parent_guard, pos } => Ok(ParentResult::Parent(OptimisticGuard::map(parent_guard, |node| node.try_internal()?.edge_at(pos))?))
        }
    }
    fn iterate_children_swips<'a>(&self, needle: &Self::PageValue, mut f: Box<dyn FnMut(&Swip<HybridLatch<BufferFrame>>) -> error::Result<bool> + 'a>) -> error::Result<()> {
        if !needle.is_leaf() {
            for pos in 0..=needle.len() {
                let swip = match needle.downcast() {
                    NodeKind::Leaf(_) => {
                        return Err(error::Error::Unwind)
                    }
                    NodeKind::Internal(node) => node.edge_at(pos)?
                };
                if !f(swip)? {
                    break
                }
            }
        }
        Ok(())
    }
    fn inspect(&self, tag: &str, value: &Self::PageValue) {
        let lower = to_u64(value.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
        let upper = to_u64(value.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
        println!("[{}] lower = {}, upper = {}", tag, lower, upper);
    }
}

fn allocate_leaf_for(dtid: DataStructureId, size: usize) -> error::Result<ExvNodeGuard> {
    match bufmgr().allocate_page_for::<Node>(dtid, size) {
        Ok((mut bf_guard, capacity)) => {
            let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| {
                PersistentBPlusTree::bf_to_page_value_mut(bf)
            });
            node_guard.init(true, capacity);
            Ok(node_guard)
        }
        Err(BufMgrError::OutOfFrames(_)) => {
            Err(error::Error::Unwind)
        }
        _ => {
            panic!("failed to allocate leaf");
        }
    }
}

fn allocate_internal_for(dtid: DataStructureId, size: usize) -> error::Result<ExvNodeGuard> {
    match bufmgr().allocate_page_for::<Node>(dtid, size) {
        Ok((mut bf_guard, capacity)) => {
            let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| {
                PersistentBPlusTree::bf_to_page_value_mut(bf)
            });
            node_guard.init(false, capacity);
            Ok(node_guard)
        }
        Err(BufMgrError::OutOfFrames(_)) => {
            Err(error::Error::Unwind)
        }
        _ => {
            panic!("failed to allocate leaf");
        }
    }
}

pub(crate) fn bf_to_node_guard(guard: OptimisticGuard<'static, BufferFrame>) -> OptNodeGuard {
    OptimisticGuard::map(guard, |bf| {
        Ok(PersistentBPlusTree::bf_to_page_value(bf))
    }).unwrap()
}

pub(crate) fn exv_bf_to_node_guard(guard: ExclusiveGuard<'static, BufferFrame>) -> ExvNodeGuard {
    ExclusiveGuard::map(guard, |bf| {
        PersistentBPlusTree::bf_to_page_value_mut(bf)
    })
}

pub(crate) fn swip_to_node_guard(guard: OptSwipGuard<'static>) -> OptNodeGuard {
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

fn to_u64(slice: &[u8]) -> u64 {
    use std::convert::TryInto;
    u64::from_be_bytes(slice.try_into().unwrap()) // FIXME debug
}

impl PersistentBPlusTree {
    pub fn new() -> Self {
        let node_guard = allocate_leaf_for(9999, 1).expect("no frame to allocate root"); // FIXME this dtid is wrong

        PersistentBPlusTree {
            root: HybridLatch::new(Swip::from_ref(node_guard.latch())),
            height: AtomicUsize::new(1),
            dtid: 0 // FIXME this method should not exist
        }
    }

    pub fn new_registered() -> Arc<PersistentBPlusTree> {
        let dtid = bufmgr().registry().reserve_dtid();
        let node_guard = allocate_leaf_for(dtid, 1).expect("no frame to allocate root");

        let tree = Arc::new(PersistentBPlusTree {
            root: HybridLatch::new(Swip::from_ref(node_guard.latch())),
            height: AtomicUsize::new(1),
            dtid
        });

        bufmgr().registry().register(dtid, tree.clone());

        tree
    }

    /// Returns the height of the tree
    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    pub(crate) fn dtid(&self) -> DataStructureId {
        self.dtid
    }

    // TODO should we be using unwind to signal frame starvation? it may be confusing
    fn allocate_leaf_for(&self, size: usize) -> error::Result<ExvNodeGuard> {
        allocate_leaf_for(self.dtid, size)
    }

    fn allocate_internal_for(&self, size: usize) -> error::Result<ExvNodeGuard> {
        allocate_internal_for(self.dtid, size)
    }

    pub(crate) fn lock_coupling(swip_guard: OptSwipGuard<'static>) -> error::Result<(OptSwipGuard<'static>, OptNodeGuard)> {
        let (swip_guard, latch) = bufmgr().resolve_swip_fast(swip_guard)?;
        let bf_guard = latch.optimistic_or_spin();
        let got_free = bf_guard.state != BfState::Hot;
        let node_guard = bf_to_node_guard(bf_guard);
        swip_guard.recheck()?;
        if got_free {
            println!("got free bug");
        }
        Ok((swip_guard, node_guard))
    }

    pub(crate) fn lock_coupling_or_unwind(swip_guard: OptSwipGuard<'static>) -> error::Result<(OptSwipGuard<'static>, OptNodeGuard)> {
        let (swip_guard, latch) = bufmgr().resolve_swip_fast(swip_guard)?;
        let bf_guard = latch.optimistic_or_unwind()?;
        let node_guard = bf_to_node_guard(bf_guard);
        swip_guard.recheck()?;
        Ok((swip_guard, node_guard))
    }

    fn find_parent_impl(&self, needle: &impl HybridGuard<Node, BufferFrame>, spin: bool) -> error::Result<ParentHandler> {
        let tree_guard = if spin {
            self.root.optimistic_or_spin()
        } else {
            self.root.optimistic_or_unwind()?
        };
        let root_latch = tree_guard.as_ref();
        let root_latch_ptr = root_latch as *const _;
        let root_guard = if spin {
            bf_to_node_guard(root_latch.optimistic_or_spin())
        } else {
            bf_to_node_guard(root_latch.optimistic_or_unwind()?)
        };

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

        // let mut path = vec!();
        // use std::convert::TryInto;

        loop {
            let (swip_guard, pos) = match target_guard.downcast() {
                NodeKind::Internal(ref internal) => {
                    match param {
                        SearchParam::Key(key) => {
                            let (pos, _) = internal.lower_bound(key)?;
                            // path.push((u64::from_be_bytes(key.try_into().unwrap()), pos));
                            let swip_guard = OptimisticGuard::map(target_guard, |node| node.try_internal()?.edge_at(pos))?;
                            // let swip = internal.edge_at(pos)?;

                            (swip_guard, pos)
                        }
                        SearchParam::Infinity => {
                            let pos = internal.base.len();
                            // path.push((987987987987987, pos));
                            // let swip = internal.upper_edge()?;
                            let swip_guard = OptimisticGuard::map(target_guard, |node| node.try_internal()?.upper_edge())?;
                            (swip_guard, pos)
                        }
                    }
                }
                NodeKind::Leaf(ref leaf) => {
                    needle.recheck()?; // This is needed to ensure this node was not merged during the search

                    target_guard.recheck()?;

                    if let Some(p) = p_guard.as_ref() {
                        p.recheck()?;
                    }

//                     let mut parent_keys = None;
//                     if let Some(p) = p_guard.as_ref() {
//                         let parent = p.try_internal()?;
//                         let parent_len = parent.base.len();
//                         let mut keys = vec!();
//                         for i in 0..parent_len {
//                             let key_slice = parent.full_key_at(i)?;
//                             let edge = parent.edge_at(i)?;
//                             keys.push((
//                                 u64::from_be_bytes(key_slice.as_slice().try_into().unwrap()),
//                                 edge
//                             ));
//                         }
//                         parent_keys = Some(keys);
//                         p.recheck()?;
//                     }

                    if let Some(tree_guard) = t_guard {
                        tree_guard.recheck()?;
                    }

//                     target_guard.recheck()?;
//
//                     needle.recheck()?; // This is needed to ensure this node was not merged during the search
//
//                     println!("path {:?}", path);
//                     let leaf_len = leaf.base.len();
//                     let mut keys = vec!();
//                     for i in 0..leaf_len {
//                         if i == 0 || i == leaf_len - 1 {
//                             let key_slice = leaf.full_key_at(i)?;
//                             let value_slice = leaf.value_at(i)?;
//                             keys.push((
//                                     u64::from_be_bytes(key_slice.as_slice().try_into().unwrap()),
//                                     u64::from_be_bytes(value_slice.try_into().unwrap()),
//                             ));
//                         }
//                     }
//                     println!("{:?}", keys);
//
//                     if let Some(tups) = parent_keys.as_ref() {
//                         let found = tups.iter().find(|(key, edge)| edge.as_raw() == needle.latch() as *const _ as u64);
//                         if let Some((key, edge)) = found {
//                             needle.recheck()?;
//                             println!("FOUND ON WRONG KEY {}", key);
//                         }
//                         let found = tups.iter().find(|(key, edge)| edge.as_raw() == target_guard.latch() as *const _ as u64);
//                         if let Some((key, edge)) = found {
//                             target_guard.recheck()?;
//                             println!("LEAF FOUND AT {}", key);
//                         }
//                     }
//
//                     let node = needle.inner();
//
//                     if node.is_leaf() {
//                         let node_len = node.len();
//                         let mut keys = vec!();
//                         for i in 0..node_len {
//                             if i == 0 || i == node_len - 1 {
//                                 let key_slice = node.try_leaf()?.full_key_at(i)?;
//                                 let value_slice = node.try_leaf()?.value_at(i)?;
//                                 keys.push((
//                                         u64::from_be_bytes(key_slice.as_slice().try_into().unwrap()),
//                                         u64::from_be_bytes(value_slice.try_into().unwrap()),
//                                 ));
//                             }
//                         }
//
//                         println!("{:?} node keys", keys);
//                     }
//                     println!("leaf_len {}, node_len {}, node is_leaf {}", leaf_len, node.len(), node.is_leaf());
//
//                     println!("leaf addr: {:?}, node addr: {:?}", std::ptr::addr_of!(leaf.base.data) as usize, std::ptr::addr_of!(node.data) as usize);
//
//                     println!("{:?} leaf lower_fence", u64::from_be_bytes(leaf.base.lower_fence()?.unwrap().try_into().unwrap()));
//                     println!("{:?} leaf upper_fence", u64::from_be_bytes(leaf.base.upper_fence()?.unwrap().try_into().unwrap()));
//
//                     println!("{:?} node lower_fence", u64::from_be_bytes(node.lower_fence()?.unwrap().try_into().unwrap()));
//                     println!("{:?} node upper_fence", u64::from_be_bytes(node.upper_fence()?.unwrap().try_into().unwrap()));
//
//                     println!("{:?} leaf pid", target_guard.as_unmapped().pid);
//                     println!("{:?} node pid", needle.as_unmapped().pid);
//
//                     println!("{:?} leaf state", target_guard.as_unmapped().state);
//                     println!("{:?} node state", needle.as_unmapped().state);
//
//                     if let Some(tups) = parent_keys {
//                         println!("{:?} parent keys", tups.iter().map(|(k, e)| k).collect::<Vec<_>>());
//                     }
//
//                     needle.recheck()?;
//                     target_guard.recheck()?;
//                     if let Some(p) = p_guard.as_ref() {
//                         p.recheck()?;
//                     }
                    // println!("reaching leaves, merges or splits are wrong");
                    // std::process::exit(3);
                    panic!("reaching leaves, merges or splits are wrong");
                    // return Err(error::Error::Unwind);
                }
            };

            if swip_guard.is_pid() { // isEvicted
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

            let (swip_guard, node_guard) = if spin {
                PersistentBPlusTree::lock_coupling(swip_guard)?
            } else {
                PersistentBPlusTree::lock_coupling_or_unwind(swip_guard)?
            };

            if swip_guard.as_raw() == needle.latch() as *const _ as u64 {
                swip_guard.recheck()?;
                return Ok(ParentHandler::Parent {
                    parent_guard: swip_to_node_guard(swip_guard),
                    pos
                });
            }

            p_guard = Some(swip_to_node_guard(swip_guard));
            target_guard = node_guard;

            if let Some(tree_guard) = t_guard.take() {
                tree_guard.recheck()?;
            }
        }
    }

    pub(crate) fn find_parent(&self, needle: &impl HybridGuard<Node, BufferFrame>) -> error::Result<ParentHandler> {
        self.find_parent_impl(needle, true)
    }

    pub(crate) fn find_parent_or_unwind(&self, needle: &impl HybridGuard<Node, BufferFrame>) -> error::Result<ParentHandler> {
        self.find_parent_impl(needle, false)
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
                    let swip_guard = OptimisticGuard::map(target_guard, |node| node.try_internal()?.edge_at(pos))?;
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

        leaf_guard.recheck()?;
        if let Some(tree_guard) = t_guard.take() {
            tree_guard.recheck()?;
        }

        if let Some((p, _)) = p_guard.as_ref() {
            p.recheck()?;
        }

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
                    let swip_guard = OptimisticGuard::map(target_guard, |node| node.try_internal()?.edge_at(pos))?;
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
            let (pos, exact) = leaf.try_leaf()?.lower_bound(key)?;
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

                let mut root_guard_x = exv_bf_to_node_guard(root_latch.exclusive());

                // TODO assert!(height == 1 || !root_guard_x.is_leaf());

                let mut new_root_guard_x = self.allocate_internal_for(1)?; // TODO set some marker so it does not get evicted

                match root_guard_x.downcast_mut() {
                    NodeKind::Internal(root_internal_node) => {
                        if root_internal_node.base.len() <= 2 {
                            return Ok(())
                        }

                        let split_pos = root_internal_node.base.len() / 2; // TODO choose a better split position if bulk loading
                        let split_key = root_internal_node.full_key_at(split_pos).expect("should exist");

                        let mut new_right_node_guard_x = self.allocate_internal_for(1)?; // Think about different size classes

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

                        let mut new_right_node_guard_x = self.allocate_leaf_for(1)?; // Think about different size classes

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
                let swip_guard = OptimisticGuard::map(parent_guard, |node| node.try_internal()?.edge_at(pos))?;
                let (swip_guard, target_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                let parent_guard = swip_to_node_guard(swip_guard);

                assert!(target_guard.latch() as *const _ == needle.latch() as *const _);

                let split_pos = target_guard.len() / 2; // TODO choose a better split position if bulk loading
                let split_key = match target_guard.downcast() {
                    NodeKind::Leaf(leaf) => leaf.full_key_at(split_pos)?,
                    NodeKind::Internal(internal) => internal.full_key_at(split_pos)?
                };

                let space_needed = parent_guard.try_internal()?.space_needed(split_key.len());
                if parent_guard.try_internal()?.has_enough_space_for(space_needed) {
                    let mut parent_guard_x = parent_guard.to_exclusive()?;
                    let mut target_guard_x = target_guard.to_exclusive()?;

                    match target_guard_x.downcast_mut() {
                        NodeKind::Internal(left_internal) => {
                            if left_internal.base.len() <= 2 {
                                return Ok(())
                            }

                            let mut new_right_node_guard_x = self.allocate_internal_for(1)?;

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

//                             let lower = to_u64(left_leaf.base.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0])); // FIXME debug
//                             let upper = to_u64(left_leaf.base.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255])); // FIXME debug
//                             let split = to_u64(split_key.as_slice());
//                             if split <= lower || split > upper {
//                                 panic!("Bad node found");
//                             }

                            let mut new_right_node_guard_x = self.allocate_leaf_for(1)?;

                            {
                                let new_right_node = new_right_node_guard_x.as_leaf_mut();
                                left_leaf.split(new_right_node, split_pos);
                            }

                            let new_right_node_edge = Swip::from_ref(new_right_node_guard_x.latch());

                            let parent_internal = parent_guard_x.as_internal_mut();

                            if pos == parent_internal.base.len() {
                                let left_edge = parent_internal.replace_upper_edge(new_right_node_edge);
//                                 println!("SPLIT upper, {} ({} - {})", split, lower, upper);
                                parent_internal.insert(split_key, left_edge);
                            } else {
                                let left_edge = parent_internal.replace_edge_at(pos, new_right_node_edge);
//                                 println!("SPLIT {}, {} ({} - {})", pos, split, lower, upper);
//                                 assert!(left_edge.as_raw() == target_guard_x.latch() as *const _ as u64);
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

                let swip_guard = OptimisticGuard::map(parent_guard, |node| node.try_internal()?.edge_at(pos))?;
                let (swip_guard, mut target_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                let mut parent_guard = swip_to_node_guard(swip_guard);

                if !target_guard.is_underfull() { // TODO is_underfull
                    target_guard.recheck()?;
                    return Ok(false);
                }

                let merge_succeded = if parent_len > 1 && pos > 0 {
                    // Try merge left
                    let swip_guard = OptimisticGuard::map(parent_guard, |node| node.try_internal()?.edge_at(pos - 1))?;
                    let (swip_guard, left_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                    parent_guard = swip_to_node_guard(swip_guard);

                    if !(left_guard.try_can_merge_with(&target_guard)?) {
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
//                                 let parent_internal = parent_guard_x.as_internal();
//                                 let pos_key = to_u64(parent_internal.full_key_at(pos).unopt().as_slice());
//                                 let prev_key = to_u64(parent_internal.full_key_at(pos - 1).unopt().as_slice());
//                                 let pos_upper = to_u64(target_leaf.base.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
//                                 let prev_upper = to_u64(left_guard_x.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
//                                 let pos_lower = to_u64(target_leaf.base.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
//                                 let prev_lower = to_u64(left_guard_x.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
// 
//                                 if pos_key != pos_upper || prev_key != prev_upper {
//                                     println!("merge left bug at {}, {}", pos - 1, pos);
//                                     println!("pos_key = {}, pos_upper = {}", pos_key, pos_upper);
//                                     println!("prev_key = {}, prev_upper = {}", prev_key, prev_upper);
//                                     println!("pos: {} - {}", pos_lower, pos_upper);
//                                     println!("prev: {} - {}", prev_lower, prev_upper);
//                                     std::process::exit(3);
//                                 }

                                if !left_guard_x.as_leaf_mut().merge(target_leaf) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
//                                         println!("MERGE LEFT {} upper", pos - 1);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {
//                                         println!(
//                                             "MERGE LEFT  ({}, {}), ({}, {})",
//                                             pos - 1, to_u64(parent_internal.full_key_at(pos - 1).unopt().as_slice()),
//                                             pos, to_u64(parent_internal.full_key_at(pos).unopt().as_slice())
//                                         );
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos - 1, left_edge);
                                    }

//                                     assert_eq!(prev_lower, to_u64(left_guard_x.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0])));
//                                     assert_eq!(pos_upper, to_u64(left_guard_x.upper_fence().unopt().unwrap()));

                                    // reclaim
                                    bufmgr().reclaim_page(ExclusiveGuard::unmap(target_guard_x));

                                    target_guard = left_guard_x.unlock(); // FIXME moving left_guard into target_guard because
                                                                          // borrow checker requires target_guard to be valid
                                                                          // even in this case where it will not get used anymore
                                    parent_guard = parent_guard_x.unlock();

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
                                        let _dropped_edge = parent_internal.replace_edge_at(pos - 1, left_edge);
                                    }

                                    // reclaim
                                    bufmgr().reclaim_page(ExclusiveGuard::unmap(target_guard_x));

                                    target_guard = left_guard_x.unlock(); // FIXME moving left_guard into target_guard because
                                                                          // borrow checker requires target_guard to be valid
                                                                          // even in this case where it will not get used anymore
                                    parent_guard = parent_guard_x.unlock();

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
                    let swip_guard = OptimisticGuard::map(parent_guard, |node| node.try_internal()?.edge_at(pos + 1))?;
                    let (swip_guard, right_guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                    parent_guard = swip_to_node_guard(swip_guard);

                    if !(right_guard.try_can_merge_with(&target_guard)?) {
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
//                                 let parent_internal = parent_guard_x.as_internal();
//                                 let pos_key = to_u64(parent_internal.full_key_at(pos).unopt().as_slice());
//                                 let next_key = if pos + 1 == parent_len {
//                                     std::u64::MAX
//                                 } else {
//                                     to_u64(parent_internal.full_key_at(pos + 1).unopt().as_slice())
//                                 };
//                                 let target_upper = to_u64(target_leaf.base.upper_fence().unopt().unwrap());
//                                 let next_upper = to_u64(right_guard_x.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
//                                 let target_lower = to_u64(target_leaf.base.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
//                                 let next_lower = to_u64(right_guard_x.lower_fence().unopt().unwrap());
// 
//                                 if pos_key != target_upper || next_key != next_upper {
//                                     println!("merge right bug at {}, {}", pos, pos + 1);
//                                     println!("pos_key = {}, pos_upper = {}", pos_key, target_upper);
//                                     println!("next_key = {}, next_upper = {}", next_key, next_upper);
//                                     println!("pos: {} - {}", target_lower, target_upper);
//                                     println!("next: {} - {}", next_lower, next_upper);
//                                     std::process::exit(3);
//                                 }

                                if !target_leaf.merge(right_guard_x.as_leaf_mut()) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    false
                                } else {
                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos + 1 == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos);
//                                         println!("MERGE RIGHT {} upper", pos);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {

//                                         println!(
//                                             "MERGE RIGHT ({}, {}), ({}, {})",
//                                             pos, to_u64(parent_internal.full_key_at(pos).unopt().as_slice()),
//                                             pos + 1, to_u64(parent_internal.full_key_at(pos + 1).unopt().as_slice())
//                                         );
                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos, left_edge);
                                    }

//                                     assert_eq!(target_lower, to_u64(target_leaf.base.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0])));
//                                     assert_eq!(next_key, to_u64(target_leaf.base.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255])));

                                    // reclaim
                                    bufmgr().reclaim_page(ExclusiveGuard::unmap(right_guard_x));

                                    target_guard_x.unlock();
                                    parent_guard = parent_guard_x.unlock();
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

                                    target_guard_x.unlock();
                                    parent_guard = parent_guard_x.unlock();
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
        let tree = PersistentBPlusTree::new_registered();
    }
}
