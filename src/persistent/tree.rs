use crate::latch::{HybridLatch, OptimisticGuard, SharedGuard, ExclusiveGuard, HybridGuard};
use crate::error::{self, NonOptimisticExt, BufMgrError};
use crate::persistent::node::SplitStrategy;
use crate::{dbg_tag, dbg_find_parent_ptr_cmp, dbg_merge_prepare, dbg_merge_left, dbg_merge_right};

use super::bufmgr::registry::DataStructureType;
use super::bufmgr::RESERVED_EPOCH;
use super::node::{Node, NodeKind, LeafNode, SplitEntryHint};
use super::bufmgr::{
    registry::{ManagedDataStructure, ParentResult, DataStructureId},
    swip::Swip, OptSwipGuard, BufferFrame,
    BfState
};

use crate::{
    dbg_find_parent_step_inner,
    dbg_find_parent_step_leaf,
    dbg_local_clear,
    dbg_split_prepare,
    dbg_split,
};

use crate::persistent::debug::to_u64;

use super::bufmgr::BufferManager;

use super::bufmgr;

use super::bufmgr::latch_ext::{
    BfOptimisticGuardExt,
    BfLatchExt
};

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering}
};

pub struct PersistentBPlusTree {
    root: HybridLatch<Swip<HybridLatch<BufferFrame>>>,
    height: AtomicUsize,
    dtid: DataStructureId,
    pub(crate) bufmgr: &'static BufferManager
}

use std::fmt;

impl fmt::Debug for PersistentBPlusTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistentBPlusTree")
            .field("root", &self.root)
            .field("dtid", &self.dtid)
            .finish()
    }
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
    fn root(&self) -> &'static HybridLatch<BufferFrame> {
        self.root.optimistic_or_spin().as_ref()
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
    fn iterate_children_swips_mut<'a>(&self, needle: &mut Self::PageValue, mut f: Box<dyn FnMut(&mut Swip<HybridLatch<BufferFrame>>) -> bool + 'a>) {
        if !needle.is_leaf() {
            for pos in 0..=needle.len() {
                let swip = match needle.downcast_mut() {
                    NodeKind::Leaf(_) => {
                        unreachable!("not leaf");
                    }
                    NodeKind::Internal(node) => node.edge_at_mut(pos)
                };
                if !f(swip) {
                    break
                }
            }
        }
    }
    fn inspect(&self, tag: &str, value: &Self::PageValue) {
        let lower = to_u64(value.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
        let upper = to_u64(value.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
        println!("[{}] lower = {}, upper = {}", tag, lower, upper);
    }
    fn debug_info(&self, value: &Self::PageValue) -> Box<dyn std::fmt::Debug + Send> {
        let lower = to_u64(value.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
        let upper = to_u64(value.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
        Box::new((lower, upper))
    }
}

fn allocate_leaf_for(bufmgr: &'static BufferManager, dtid: DataStructureId, size: usize) -> error::Result<ExvNodeGuard> {
    match bufmgr.allocate_page_for::<Node>(dtid, size) {
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

fn allocate_internal_for(bufmgr: &'static BufferManager, dtid: DataStructureId, size: usize) -> error::Result<ExvNodeGuard> {
    match bufmgr.allocate_page_for::<Node>(dtid, size) {
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

pub(crate) fn shr_bf_to_node_guard(guard: SharedGuard<'static, BufferFrame>) -> ShrNodeGuard {
    SharedGuard::map(guard, |bf| {
        PersistentBPlusTree::bf_to_page_value(bf)
    })
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

enum TreeOrParent<'a> {
    Tree(OptimisticGuard<'a, Swip<HybridLatch<BufferFrame>>>),
    Parent(OptNodeGuard, usize)
}

fn reaching_leaves_debug(
    needle: &impl HybridGuard<Node, BufferFrame>,
    target_guard: &OptNodeGuard,
    leaf: &LeafNode,
    p_guard: TreeOrParent) -> error::Result<()> {
    use std::convert::TryInto;

    let mut parent_keys = None;
    if let TreeOrParent::Parent(p, _) = &p_guard {
        let parent = p.try_internal()?;
        let parent_len = parent.base.len();
        let mut keys = vec!();
        for i in 0..parent_len {
            let key_slice = parent.full_key_at(i)?;
            let edge = parent.edge_at(i)?.clone();
            keys.push((
                    u64::from_be_bytes(key_slice.as_slice().try_into().unwrap()),
                    edge
                    ));
        }
        parent_keys = Some(keys);
        p.recheck()?;
    }

    let leaf_len = leaf.base.len();
    let mut leaf_keys = vec!();
    for i in 0..leaf_len {
        if i == 0 || i == leaf_len - 1 {
            let key_slice = leaf.full_key_at(i)?;
            let value_slice = leaf.value_at(i)?;
            leaf_keys.push((
                    to_u64(key_slice.as_slice()),
                    to_u64(value_slice),
            ));
        }
    }

    let leaf_lower_fence = to_u64(leaf.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
    let leaf_upper_fence = to_u64(leaf.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));

    let node = needle.inner();

    let mut node_keys = vec!();
    let node_len = node.len();
    let node_is_leaf = node.is_leaf();
    if node_is_leaf {
        for i in 0..node_len {
            if i == 0 || i == node_len - 1 {
                let key_slice = node.try_leaf()?.full_key_at(i)?;
                let value_slice = node.try_leaf()?.value_at(i)?;
                node_keys.push((
                        to_u64(key_slice.as_slice()),
                        to_u64(value_slice),
                        ));
            }
        }
    }

    let node_lower_fence = to_u64(node.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
    let node_upper_fence = to_u64(node.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));

    let leaf_pid = target_guard.as_unmapped().pid;
    let node_pid = needle.as_unmapped().pid;

    let leaf_state = target_guard.as_unmapped().state;
    let node_state = needle.as_unmapped().state;

    // TODO Extra checks needed?

    match p_guard {
        TreeOrParent::Tree(guard) => {
            guard.recheck()?;
        },
        TreeOrParent::Parent(guard, pos) => {
            guard.recheck()?;
        }
    }

    target_guard.recheck()?;

    needle.recheck()?;

    dbg_find_parent_step_leaf!(target_guard, needle.inner().upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));

    crate::dbg_global_report!();
    crate::dbg_local_report!();

    if let Some(tups) = parent_keys.as_ref() {
        let found = tups.iter().find(|(key, edge)| edge.as_hot_ptr() == needle.latch() as *const _ as u64);
        if let Some((key, edge)) = found {
            println!("FOUND ON WRONG KEY {}", key);
        }
        let found = tups.iter().find(|(key, edge)| edge.as_hot_ptr() == target_guard.latch() as *const _ as u64);
        if let Some((key, edge)) = found {
            println!("LEAF FOUND AT {}", key);
        }
    }

    eprintln!("{:?} node keys", node_keys);

    eprintln!("leaf_len {}, node_len {}, node is_leaf {}", leaf_len, node_len, node_is_leaf);

    eprintln!("{:?} leaf lower_fence", leaf_lower_fence);
    eprintln!("{:?} leaf upper_fence", leaf_upper_fence);

    eprintln!("{:?} node lower_fence", node_lower_fence);
    eprintln!("{:?} node upper_fence", node_upper_fence);

    eprintln!("{:?} leaf pid", leaf_pid);
    eprintln!("{:?} node pid", node_pid);

    eprintln!("{:?} leaf state", leaf_state);
    eprintln!("{:?} node state", node_state);

    if let Some(tups) = parent_keys {
        eprintln!("{:?} parent keys", tups.iter().map(|(k, e)| k).collect::<Vec<_>>());
    }
    Ok(())
}

impl PersistentBPlusTree {
    pub fn load_with(bufmgr: &'static BufferManager, dtid: DataStructureId) -> Option<Arc<PersistentBPlusTree>> {
        bufmgr.load_data_structure(dtid, DataStructureType::BTREE, |dtid, guard: ExclusiveGuard<Node, BufferFrame>| {
            PersistentBPlusTree {
                root: HybridLatch::new(Swip::from_ref(guard.latch())),
                height: AtomicUsize::new(1),
                dtid,
                bufmgr,
            }
        })
    }

    pub fn load(dtid: DataStructureId) -> Option<Arc<PersistentBPlusTree>> {
        PersistentBPlusTree::load_with(bufmgr(), dtid)
    }

    pub fn create_with(bufmgr: &'static BufferManager) -> Option<(DataStructureId, Arc<PersistentBPlusTree>)> {
        bufmgr.create_data_structure(DataStructureType::BTREE, |dtid, mut guard: ExclusiveGuard<Node, BufferFrame>, capacity| {
            guard.init(true, capacity);
            PersistentBPlusTree {
                root: HybridLatch::new(Swip::from_ref(guard.latch())),
                height: AtomicUsize::new(1),
                dtid,
                bufmgr
            }
        })
    }

    pub fn create() -> Option<(DataStructureId, Arc<PersistentBPlusTree>)> {
        PersistentBPlusTree::create_with(bufmgr())
    }

    pub fn evict(self: Arc<Self>) -> Result<(), Arc<Self>> {
        self.bufmgr.evict_data_structure(self.dtid, DataStructureType::BTREE, self)
    }

    pub fn root_hints(&self) -> Vec<u32> {
        let tree_guard = self.root.shared();
        let root_guard = tree_guard.as_ref().shared();
        let root_node = shr_bf_to_node_guard(root_guard);
        root_node.hints().to_vec()
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
        allocate_leaf_for(self.bufmgr, self.dtid, size)
    }

    fn allocate_internal_for(&self, size: usize) -> error::Result<ExvNodeGuard> {
        allocate_internal_for(self.bufmgr, self.dtid, size)
    }

    pub(crate) fn lock_coupling(bufmgr: &'static BufferManager, swip_guard: OptSwipGuard<'static>) -> error::Result<(OptSwipGuard<'static>, OptNodeGuard)> {
        let (swip_guard, latch) = bufmgr.resolve_swip_fast(swip_guard, true)?;
        let bf_guard = latch.optimistic_or_spin();
        debug_assert_ne!(BfState::Free, bf_guard.state);
        let node_guard = bf_to_node_guard(bf_guard);
        swip_guard.recheck()?;
        Ok((swip_guard, node_guard))
    }

    pub(crate) fn lock_coupling_or_unwind(bufmgr: &'static BufferManager, swip_guard: OptSwipGuard<'static>) -> error::Result<(OptSwipGuard<'static>, OptNodeGuard)> {
        let (swip_guard, latch) = bufmgr.resolve_swip_fast(swip_guard, false)?;
        let bf_guard = latch.optimistic_or_unwind()?;
        debug_assert_ne!(BfState::Free, bf_guard.state);
        let node_guard = bf_to_node_guard(bf_guard);
        swip_guard.recheck()?;
        Ok((swip_guard, node_guard))
    }

    fn find_parent_impl(&self, needle: &impl HybridGuard<Node, BufferFrame>, spin: bool) -> error::Result<ParentHandler> {
        dbg_local_clear!();
        dbg_tag!(1);
        let tree_guard = if spin {
            self.root.optimistic_or_spin()
        } else {
            self.root.optimistic_or_unwind()?
        };
        dbg_tag!(2);
        let root_latch = tree_guard.as_ref();
        let root_latch_ptr = root_latch as *const _;
        let root_guard = if spin {
            bf_to_node_guard(root_latch.optimistic_or_spin())
        } else {
            let res = root_latch.optimistic_or_unwind();
            bf_to_node_guard(res?)
        };
        dbg_tag!(3);

        if needle.latch() as *const _ == root_latch_ptr {
            tree_guard.recheck()?;
            return Ok(ParentHandler::Root { tree_guard })
        }
        dbg_tag!(4);

        enum SearchParam<'a> {
            Key(&'a [u8]),
            Infinity
        }

        let param = match needle.inner().upper_fence()? {
            Some(key) => SearchParam::Key(key),
            None => SearchParam::Infinity
        };
        dbg_tag!(5);

        let mut p_guard = TreeOrParent::Tree(tree_guard);
        let mut target_guard = root_guard;

        loop {
            let (swip_guard, pos) = match target_guard.downcast() {
                NodeKind::Internal(ref internal) => {
                    match param {
                        SearchParam::Key(key) => {
                            let (pos, _) = internal.lower_bound(key)?;
                            dbg_find_parent_step_inner!(target_guard, pos, key);
                            let swip_guard = OptimisticGuard::map(target_guard, |node| node.try_internal()?.edge_at(pos))?;

                            (swip_guard, pos)
                        }
                        SearchParam::Infinity => {
                            let pos = internal.base.len();
                            let swip_guard = OptimisticGuard::map(target_guard, |node| node.try_internal()?.upper_edge())?;
                            (swip_guard, pos)
                        }
                    }
                }
                NodeKind::Leaf(ref leaf) => {
                    needle.recheck()?; // This is needed to ensure this node was not merged during the search
                    target_guard.recheck()?;

                    match &p_guard {
                        TreeOrParent::Tree(guard) => {
                            guard.recheck()?;
                        },
                        TreeOrParent::Parent(guard, _pos) => {
                            guard.recheck()?;
                        }
                    }

                    reaching_leaves_debug(needle, &target_guard, leaf, p_guard)?;

                    panic!("reaching leaves, merges or splits are wrong");
                }
            };

            if swip_guard.is_pid() { // isEvicted
                // There may not be any evicted swip in the root to needle path
                return Err(error::Error::Unwind);
            }

            dbg_find_parent_ptr_cmp!(swip_guard, needle);

            if swip_guard.as_hot_ptr() == needle.latch() as *const _ as u64 {
                swip_guard.recheck()?;
                return Ok(ParentHandler::Parent {
                    parent_guard: swip_to_node_guard(swip_guard),
                    pos
                });
            }

            let (swip_guard, node_guard) = if spin {
                PersistentBPlusTree::lock_coupling(self.bufmgr, swip_guard)?
            } else {
                PersistentBPlusTree::lock_coupling_or_unwind(self.bufmgr, swip_guard)?
            };

            if let TreeOrParent::Tree(ref guard) = p_guard {
                guard.recheck()?;
            }

            p_guard = TreeOrParent::Parent(swip_to_node_guard(swip_guard), pos);

            target_guard = node_guard;
        }
    }

    pub(crate) fn find_parent(&self, needle: &impl HybridGuard<Node, BufferFrame>) -> error::Result<ParentHandler> {
        self.find_parent_impl(needle, true)
    }

    pub(crate) fn find_parent_or_unwind(&self, needle: &impl HybridGuard<Node, BufferFrame>) -> error::Result<ParentHandler> {
        self.find_parent_impl(needle, false)
    }

    pub fn find_leaf_and_parent<K: AsRef<[u8]>>(&self, key: K) -> error::Result<(OptNodeGuard, Option<(OptNodeGuard, usize)>)> {
        let key = key.as_ref();
        let tree_guard = self.root.optimistic_or_spin();
        let root_latch = tree_guard.as_ref();
        let root_guard = bf_to_node_guard(root_latch.optimistic_or_spin());

        let mut p_guard = TreeOrParent::Tree(tree_guard);
        let mut target_guard = root_guard;

        let mut level = 0u16;

        dbg_local_clear!();

        let leaf_guard = loop {
            let (swip_guard, pos) = match target_guard.downcast() {
                NodeKind::Internal(internal) => {
                    let (pos, _) = internal.lower_bound(key)?;
                    dbg_find_parent_step_inner!(target_guard, pos, key);
                    let swip_guard = OptimisticGuard::map(target_guard, |node| node.try_internal()?.edge_at(pos))?;
                    (swip_guard, pos)
                }
                NodeKind::Leaf(_leaf) => {
                    dbg_find_parent_step_leaf!(target_guard, key);
                    break target_guard;
                }
            };

            // TODO if level == height - 1 use shared mode

            let (swip_guard, node_guard) = PersistentBPlusTree::lock_coupling(self.bufmgr, swip_guard)?;
            if let TreeOrParent::Tree(ref guard) = p_guard {
                guard.recheck()?;
            }

            p_guard = TreeOrParent::Parent(swip_to_node_guard(swip_guard), pos);
            target_guard = node_guard;

            level += 1;
        };

        let p_res = match p_guard {
            TreeOrParent::Tree(guard) => {
                guard.recheck()?;
                None
            },
            TreeOrParent::Parent(guard, pos) => {
                guard.recheck()?;
                Some((guard, pos))
            }
        };

        leaf_guard.recheck()?;

        Ok((leaf_guard, p_res))
    }

    pub fn find_leaf<K: AsRef<[u8]>>(&self, key: K) -> error::Result<OptNodeGuard> {
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

            let (swip_guard, node_guard) = PersistentBPlusTree::lock_coupling(self.bufmgr, swip_guard)?;
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
            let exclusive_leaf = leaf.to_exclusive_bf()?;
            Ok((exclusive_leaf, parent_opt))
        })
    }

    pub(crate) fn find_last_exclusive_leaf_and_optimistic_parent(&self) -> (ExvNodeGuard, Option<(OptNodeGuard, usize)>) {
        retry(|| {
            let (leaf, parent_opt) = self.find_last_leaf_and_parent()?;
            let exclusive_leaf = leaf.to_exclusive_bf()?;
            Ok((exclusive_leaf, parent_opt))
        })
    }

    pub(crate) fn find_exact_exclusive_leaf_and_optimistic_parent<K: AsRef<[u8]>>(&self, key: K) -> Option<((ExvNodeGuard, usize), Option<(OptNodeGuard, usize)>)> {
        let key = key.as_ref();
        retry(|| {
            let (leaf, parent_opt) = self.find_leaf_and_parent(key)?;
            let (pos, exact) = leaf.try_leaf()?.lower_bound(key)?;
            if exact {
                let exclusive_leaf = leaf.to_exclusive_bf()?;
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
            let exclusive_leaf = leaf.to_exclusive_bf()?;
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
                        }
                    }
                }
            }
        }
    }

    // Care must be taken when allocating a node in this function, any nodes allocated and not used
    // must be reclaimed. Take extra care with unwind points after allocating.
    pub(crate) fn try_split(&self, needle: &OptNodeGuard, entry_hint: Option<SplitEntryHint>) -> error::Result<()> {
        let parent_handler = self.find_parent(needle)?;

        match parent_handler {
            ParentHandler::Root { tree_guard } => {
                let mut tree_guard_x = tree_guard.to_exclusive()?;

                let root_latch = tree_guard_x.as_ref();

                let mut root_guard_x = exv_bf_to_node_guard(root_latch.exclusive_bf());

                // TODO assert!(height == 1 || !root_guard_x.is_leaf());

                match root_guard_x.downcast_mut() {
                    NodeKind::Internal(root_internal_node) => {
                        if root_internal_node.base.len() <= 2 {
                            return Ok(())
                        }

                        let mut new_root_guard_x = self.allocate_internal_for(1)?;
                        new_root_guard_x.as_unmapped_mut().epoch.store(RESERVED_EPOCH , Ordering::Release); // This epoch prevents eviction

                        let split_pos = root_internal_node.base.len() / 2; // TODO choose a better split position if bulk loading
                        let split_key = root_internal_node.full_key_at(split_pos).expect("should exist");

                        let mut new_right_node_guard_x = match self.allocate_internal_for(1) {
                            Ok(f) => f,
                            Err(_) => {
                                self.bufmgr.reclaim_page(ExclusiveGuard::unmap(new_root_guard_x));
                                return Err(error::Error::Unwind);
                            }
                        };

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

                        let new_root_node_edge = Swip::from_ref(new_root_guard_x.latch());
                        *tree_guard_x = new_root_node_edge;
                        self.height.fetch_add(1, Ordering::Relaxed);
                    }
                    NodeKind::Leaf(root_leaf_node) => {
                        dbg_split_prepare!(_split, root_leaf_node);

                        // TODO choose a better split position if bulk loading
                        let split_strategy = root_leaf_node.split_heuristic(entry_hint).unopt();

                        match &split_strategy {
                            SplitStrategy::SplitAt { key, split_pos, new_left_size, right_size } => {
                                let mut new_root_guard_x = self.allocate_internal_for(1)?;
                                new_root_guard_x.as_unmapped_mut().epoch.store(RESERVED_EPOCH , Ordering::Release); // This epoch prevents eviction

                                let mut new_right_node_guard_x = match self.allocate_leaf_for(*right_size) {
                                    Ok(f) => f,
                                    Err(_) => {
                                        self.bufmgr.reclaim_page(ExclusiveGuard::unmap(new_root_guard_x));
                                        return Err(error::Error::Unwind);
                                    }
                                };

                                let mut new_left = match new_left_size {
                                    Some(left_size) => {
                                        match self.allocate_leaf_for(*left_size) {
                                            Ok(f) => Some(f),
                                            Err(_) => {
                                                self.bufmgr.reclaim_page(ExclusiveGuard::unmap(new_root_guard_x));
                                                self.bufmgr.reclaim_page(ExclusiveGuard::unmap(new_right_node_guard_x));
                                                return Err(error::Error::Unwind);
                                            }
                                        }
                                    },
                                    None => None
                                };

                                {
                                    let new_right_node = new_right_node_guard_x.as_leaf_mut();
                                    root_leaf_node.split(
                                        new_right_node,
                                        new_left
                                            .as_deref_mut()
                                            .map(|n| n.as_leaf_mut()),
                                        &split_strategy
                                    );
                                }

                                let split_key = key.as_slice();

                                let old_root_edge = Swip::clone(&tree_guard_x);
                                let new_right_node_edge = Swip::from_ref(new_right_node_guard_x.latch());

                                if let Some(mut new_left) = new_left {
                                    let new_left_edge = Swip::from_ref(new_left.latch());
                                    let _new_left_leaf = new_left.as_leaf_mut();
                                    dbg_split!(_split, _new_left_leaf, new_right_node_guard_x.as_leaf_mut(), 0, 0, &split_key);

                                    {
                                        let new_root = new_root_guard_x.as_internal_mut();
                                        new_root.insert(split_key, new_left_edge).expect("must have space");
                                        new_root.set_upper_edge(new_right_node_edge);
                                    }

                                    let new_root_node_edge = Swip::from_ref(new_root_guard_x.latch());
                                    *tree_guard_x = new_root_node_edge;
                                    self.height.fetch_add(1, Ordering::Relaxed);

                                    self.bufmgr
                                        .reclaim_page(ExclusiveGuard::unmap(root_guard_x));

                                } else {
                                    dbg_split!(_split, root_leaf_node, new_right_node_guard_x.as_leaf_mut(), 0, 0, &split_key);

                                    {
                                        let new_root = new_root_guard_x.as_internal_mut();
                                        new_root.insert(split_key, old_root_edge).expect("must have space");
                                        new_root.set_upper_edge(new_right_node_edge);
                                    }

                                    let new_root_node_edge = Swip::from_ref(new_root_guard_x.latch());
                                    *tree_guard_x = new_root_node_edge;
                                    self.height.fetch_add(1, Ordering::Relaxed);
                                }

                            }
                            SplitStrategy::Grow { new_size } => {
                                // TODO dbg_grow!(...)

                                let mut new_root_guard_x = self.allocate_leaf_for(*new_size)?;

                                root_leaf_node.copy_to(new_root_guard_x.as_leaf_mut());

                                let new_root_node_edge = Swip::from_ref(new_root_guard_x.latch());

                                *tree_guard_x = new_root_node_edge;
                                self.height.fetch_add(1, Ordering::Relaxed);

                                self.bufmgr.reclaim_page(ExclusiveGuard::unmap(root_guard_x));
                            }
                        }
                    }
                }
            },
            ParentHandler::Parent { parent_guard, pos } => {
                let swip_guard = OptimisticGuard::map(parent_guard, |node| node.try_internal()?.edge_at(pos))?;
                let (swip_guard, target_guard) = PersistentBPlusTree::lock_coupling(self.bufmgr, swip_guard)?;
                let parent_guard = swip_to_node_guard(swip_guard);

                assert!(target_guard.latch() as *const _ == needle.latch() as *const _);

                let mut parent_guard_x = parent_guard.to_exclusive_bf()?;
                let mut target_guard_x = target_guard.to_exclusive_bf()?;

                // TODO choose a better split position if bulk loading
                let split_strategy = match target_guard_x.downcast() {
                    NodeKind::Leaf(leaf) => {
                        leaf.split_heuristic(entry_hint)?
                    },
                    NodeKind::Internal(internal) => {
                        let split_pos = target_guard_x.len() / 2;
                        let split_key = internal.full_key_at(split_pos)?;
                        SplitStrategy::SplitAt { key: split_key, split_pos, new_left_size: None, right_size: 1 }
                    }
                };

                let space_needed = match &split_strategy {
                    SplitStrategy::SplitAt { key, .. } => parent_guard_x.try_internal()?.space_needed(split_strategy.split_key().len()),
                    SplitStrategy::Grow { .. } => 0
                };

                if parent_guard_x.try_internal()?.has_enough_space_for(space_needed) {
                    match target_guard_x.downcast_mut() {
                        NodeKind::Internal(left_internal) => {
                            if left_internal.base.len() <= 2 {
                                return Ok(())
                            }

                            let (split_key, split_pos, right_size) = match split_strategy {
                                SplitStrategy::SplitAt { key, split_pos, new_left_size: _, right_size } => {
                                    (key, split_pos, right_size)
                                }
                                SplitStrategy::Grow { .. } => {
                                    unimplemented!("internal grow");
                                }
                            };

                            let mut new_right_node_guard_x = self.allocate_internal_for(right_size)?;

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
                            dbg_split_prepare!(_split, left_leaf);

                            match &split_strategy {
                                SplitStrategy::SplitAt { key, split_pos, new_left_size, right_size } => {
                                    let mut new_right_node_guard_x = self.allocate_leaf_for(*right_size)?;

                                    let mut new_left = match new_left_size {
                                        Some(left_size) => {
                                            match self.allocate_leaf_for(*left_size) {
                                                Ok(f) => Some(f),
                                                Err(_) => {
                                                    self.bufmgr.reclaim_page(ExclusiveGuard::unmap(new_right_node_guard_x));
                                                    return Err(error::Error::Unwind);
                                                }
                                            }
                                        },
                                        None => None
                                    };

                                    {
                                        let new_right_node = new_right_node_guard_x.as_leaf_mut();
                                        left_leaf.split(
                                            new_right_node,
                                            new_left
                                                .as_deref_mut()
                                                .map(|n| n.as_leaf_mut()),
                                            &split_strategy
                                        );
                                    }

                                    let split_key = key.as_slice();

                                    let new_right_node_edge = Swip::from_ref(new_right_node_guard_x.latch());

                                    let parent_internal = parent_guard_x.as_internal_mut();

                                    if let Some(mut new_left) = new_left {
                                        let new_left_edge = Swip::from_ref(new_left.latch());
                                        let _new_left_leaf = new_left.as_leaf_mut();

                                        dbg_split!(_split, _new_left_leaf, new_right_node_guard_x.as_leaf_mut(), parent_internal.base.len(), pos, &split_key);
                                        if pos == parent_internal.base.len() {
                                            let _old_left_edge = parent_internal.replace_upper_edge(new_right_node_edge);
                                            parent_internal.insert(split_key, new_left_edge);

                                            self.bufmgr.reclaim_page(ExclusiveGuard::unmap(target_guard_x));
                                        } else {
                                            let _old_left_edge = parent_internal.replace_edge_at(pos, new_right_node_edge);
                                            parent_internal.insert(split_key, new_left_edge);
                                            self.bufmgr.reclaim_page(ExclusiveGuard::unmap(target_guard_x));
                                        }

                                    } else {
                                        dbg_split!(_split, left_leaf, new_right_node_guard_x.as_leaf_mut(), parent_internal.base.len(), pos, &split_key);
                                        if pos == parent_internal.base.len() {
                                            let left_edge = parent_internal.replace_upper_edge(new_right_node_edge);
                                            parent_internal.insert(split_key, left_edge);
                                        } else {
                                            let left_edge = parent_internal.replace_edge_at(pos, new_right_node_edge);
                                            parent_internal.insert(split_key, left_edge);
                                        }
                                    }
                                }
                                SplitStrategy::Grow { new_size } => {
                                    let mut new_leaf_guard_x = self.allocate_leaf_for(*new_size)?;

                                    left_leaf.copy_to(new_leaf_guard_x.as_leaf_mut());

                                    let new_leaf_node_edge = Swip::from_ref(new_leaf_guard_x.latch());

                                    let parent_internal = parent_guard_x.as_internal_mut();

                                    // TODO dbg_grow!(...)
                                    if pos == parent_internal.base.len() {
                                        let _old_left_edge = parent_internal.replace_upper_edge(new_leaf_node_edge);
                                        self.bufmgr.reclaim_page(ExclusiveGuard::unmap(target_guard_x));
                                    } else {
                                        let _old_left_edge = parent_internal.replace_edge_at(pos, new_leaf_node_edge);
                                        self.bufmgr.reclaim_page(ExclusiveGuard::unmap(target_guard_x));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    let parent_guard = parent_guard_x.unlock();
                    target_guard_x.unlock();
                    self.try_split(&parent_guard, None)?;
                }
            }
        }
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
                let (swip_guard, mut target_guard) = PersistentBPlusTree::lock_coupling(self.bufmgr, swip_guard)?;
                let mut parent_guard = swip_to_node_guard(swip_guard);

                if !target_guard.is_underfull() {
                    target_guard.recheck()?;
                    return Ok(false);
                }

                let merge_succeded = if parent_len > 1 && pos > 0 {
                    // Try merge left
                    let swip_guard = OptimisticGuard::map(parent_guard, |node| node.try_internal()?.edge_at(pos - 1))?;
                    let (swip_guard, left_guard) = PersistentBPlusTree::lock_coupling(self.bufmgr, swip_guard)?;
                    parent_guard = swip_to_node_guard(swip_guard);

                    if !(left_guard.try_can_merge_with(&target_guard)?) {
                        left_guard.recheck()?;
                        target_guard.recheck()?;
                        false
                    } else {
                        let mut parent_guard_x = parent_guard.to_exclusive_bf()?;
                        let mut target_guard_x = target_guard.to_exclusive_bf()?;
                        let mut left_guard_x = left_guard.to_exclusive_bf()?;

                        let r_latch = target_guard_x.latch() as *const _ as u64;

                        match target_guard_x.downcast_mut() {
                            NodeKind::Leaf(ref mut target_leaf) => {
                                assert!(left_guard_x.is_leaf());

                                dbg_merge_prepare!(_merge, left_guard_x.as_leaf_mut(), target_leaf, r_latch);

                                if !left_guard_x.as_leaf_mut().merge(target_leaf) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
                                    false
                                } else {
                                    dbg_merge_left!(_merge, left_guard_x.as_leaf_mut(), parent_guard_x, pos);

                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                        assert_eq!(_dropped_edge.as_hot_ptr(), target_guard_x.latch() as *const _ as u64);
                                    } else {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos - 1, left_edge);
                                        assert_eq!(_dropped_edge.as_hot_ptr(), target_guard_x.latch() as *const _ as u64);
                                    }

                                    // reclaim
                                    self.bufmgr.reclaim_page(ExclusiveGuard::unmap(target_guard_x));

                                    target_guard = left_guard_x.unlock(); // FIXME moving left_guard into target_guard because
                                                                          // borrow checker requires target_guard to be valid
                                                                          // even in this case where it will not get used anymore
                                    parent_guard = parent_guard_x.unlock();

                                    true
                                }
                            }
                            NodeKind::Internal(target_internal) => {
                                assert!(!left_guard_x.is_leaf());

                                dbg_merge_prepare!(_merge, left_guard_x.as_internal_mut(), target_internal, r_latch);

                                if !left_guard_x.as_internal_mut().merge(target_internal) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard = target_guard_x.unlock();
                                    false
                                } else {
                                    dbg_merge_left!(_merge, left_guard_x.as_internal_mut(), parent_guard_x, pos);

                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {
                                        let left_edge = parent_internal.remove_edge_at(pos - 1);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos - 1, left_edge);
                                    }

                                    // reclaim
                                    self.bufmgr.reclaim_page(ExclusiveGuard::unmap(target_guard_x));

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
                    let (swip_guard, right_guard) = PersistentBPlusTree::lock_coupling(self.bufmgr, swip_guard)?;
                    parent_guard = swip_to_node_guard(swip_guard);

                    if !(target_guard.try_can_merge_with(&right_guard)?) {
                        right_guard.recheck()?;
                        target_guard.recheck()?;
                        false
                    } else {
                        let mut parent_guard_x = parent_guard.to_exclusive_bf()?;
                        let mut target_guard_x = target_guard.to_exclusive_bf()?;
                        let mut right_guard_x = right_guard.to_exclusive_bf()?;

                        let r_latch = right_guard_x.latch() as *const _ as u64;

                        match target_guard_x.downcast_mut() {
                            NodeKind::Leaf(ref mut target_leaf) => {
                                assert!(right_guard_x.is_leaf());

                                dbg_merge_prepare!(_merge, target_leaf, right_guard_x.as_leaf_mut(), r_latch);

                                if !target_leaf.merge(right_guard_x.as_leaf_mut()) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    false
                                } else {
                                    dbg_merge_right!(_merge, target_leaf, parent_guard_x, pos);

                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos + 1 == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {

                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos, left_edge);
                                    }

                                    // reclaim
                                    self.bufmgr.reclaim_page(ExclusiveGuard::unmap(right_guard_x));

                                    target_guard_x.unlock();
                                    parent_guard = parent_guard_x.unlock();
                                    true
                                }
                            }
                            NodeKind::Internal(target_internal) => {
                                assert!(!right_guard_x.is_leaf());

                                dbg_merge_prepare!(_merge, target_internal, right_guard_x.as_internal_mut(), r_latch);

                                if !target_internal.merge(right_guard_x.as_internal_mut()) {
                                    parent_guard = parent_guard_x.unlock();
                                    target_guard_x.unlock();
                                    false
                                } else {
                                    dbg_merge_right!(_merge, target_internal, parent_guard_x, pos);

                                    let parent_internal = parent_guard_x.as_internal_mut();
                                    if pos + 1 == parent_len {
                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_upper_edge(left_edge);
                                    } else {
                                        let left_edge = parent_internal.remove_edge_at(pos);
                                        let _dropped_edge = parent_internal.replace_edge_at(pos, left_edge);
                                    }

                                    // reclaim
                                    self.bufmgr.reclaim_page(ExclusiveGuard::unmap(right_guard_x));

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

    pub fn raw_iter<'t>(&'t self) -> super::iter::RawSharedIter<'t> {
        super::iter::RawSharedIter::new(self)
    }

    pub fn raw_iter_mut<'t>(&'t self) -> super::iter::RawExclusiveIter<'t> {
        super::iter::RawExclusiveIter::new(self)
    }
}


#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use super::PersistentBPlusTree;
    use crate::{persistent::{ensure_global_bufmgr, bufmgr}, latch::HybridGuard};

    #[test]
    fn persistent_bplustree_init() {
        ensure_global_bufmgr("/tmp/state.db", 1 * 1024 * 1024).unwrap();
        let (_, tree) = PersistentBPlusTree::create().expect("ok");

        let leaf = tree.find_leaf("test").unwrap();
        let addr = std::ptr::addr_of!(*leaf.as_unmapped().page) as u64;
        println!("page addr = {}, align 4096 = {}, 128 = {}", addr, addr % 4096, addr % 128);
        let addr = std::ptr::addr_of!(*leaf) as u64;
        println!("node addr = {}, align 4096 = {}, 128 = {}", addr, addr % 4096, addr % 128);
        let addr = std::ptr::addr_of!(leaf.data) as u64;
        println!("data addr = {}, align 4096 = {}, 128 = {}", addr, addr % 4096, addr % 128);
    }

    #[test]
    fn persistent_bplustree_evict_and_load() {
        use rand::prelude::*;

        std::fs::remove_file("/tmp/evict.db").expect("failed to clear db");
        ensure_global_bufmgr("/tmp/evict.db", 100 * 1024 * 1024).unwrap();
        let (dtid, tree) = PersistentBPlusTree::create().expect("ok");

        let n = 1_000_000;
        let mut rng = rand::thread_rng();
        let mut data: Vec<usize> = (0..n).collect();
        data.shuffle(&mut rng);

        for i in 0..n {
            let mut iter = tree.raw_iter_mut();
            iter.insert(data[i].to_be_bytes(), data[i].to_be_bytes());
        }

        tree.evict().expect("evicted");

        let tree = PersistentBPlusTree::load(dtid).expect("ok");

        data.sort();

        let mut iter = tree.raw_iter();
        iter.seek_to_first();

        for datum in data {
            let v: usize = usize::from_be_bytes(iter.next().expect("early eof").1.try_into().expect("corrupted"));
            assert_eq!(datum, v);
        }

        assert!(iter.next().is_none());

        bufmgr().persist_metadata().expect("should persist");
    }
}
