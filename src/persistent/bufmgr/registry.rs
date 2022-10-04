use std::sync::{
    Arc,
    atomic::{
        AtomicU64,
        Ordering
    }
};
use super::{BufferFrame, OptSwipGuard, Swip};
use bplustree::{error, latch::{HybridLatch, OptimisticGuard, HybridGuard}};

pub(crate) enum ParentResult {
    Root,
    Parent(OptSwipGuard<'static>)
}

pub(crate) trait ManagedDataStructure {
    type PageValue;
    fn bf_to_page_value(bf: &BufferFrame) -> &Self::PageValue;
    fn bf_to_page_value_mut(bf: &mut BufferFrame) -> &mut Self::PageValue;
    fn find_parent(&self, needle: &impl HybridGuard<Self::PageValue, BufferFrame>) -> error::Result<ParentResult>;
    fn iterate_children_swips<'a>(&self, needle: &Self::PageValue, f: Box<dyn FnMut(&Swip<HybridLatch<BufferFrame>>) -> error::Result<bool> + 'a>) -> error::Result<()>;
    fn inspect(&self, tag: &str, value: &Self::PageValue) {}
}

pub(crate) trait ErasedDataStructure {
    fn find_parent(&self, needle: OptimisticGuard<'static, BufferFrame>) -> error::Result<(ParentResult, OptimisticGuard<'static, BufferFrame>)>;
    fn iterate_children_swips<'a>(&self, needle: &BufferFrame, f: Box<dyn FnMut(&Swip<HybridLatch<BufferFrame>>) -> error::Result<bool> + 'a>) -> error::Result<()>;
    // fn scan_children_swips<A>(&self, needle: &OptimisticGuard<'static, BufferFrame>, acc: A, f: Box<dyn Fn(&Swip<HybridLatch<BufferFrame>>) -> error::Result<bool>>) -> error::Result<()>;
    fn inspect(&self, tag: &str, needle: &BufferFrame);
}

impl <T, U: ManagedDataStructure<PageValue = T>> ErasedDataStructure for U {
    fn find_parent(&self, needle: OptimisticGuard<'static, BufferFrame>) -> error::Result<(ParentResult, OptimisticGuard<'static, BufferFrame>)> {
        let mapped = OptimisticGuard::map(needle, |bf| Ok(U::bf_to_page_value(bf)))?;
        let parent_res = self.find_parent(&mapped)?;
        let needle = OptimisticGuard::unmap(mapped);
        Ok((parent_res, needle))
    }
    fn iterate_children_swips<'a>(&self, needle: &BufferFrame, f: Box<dyn FnMut(&Swip<HybridLatch<BufferFrame>>) -> error::Result<bool> + 'a>) -> error::Result<()> {
        self.iterate_children_swips(U::bf_to_page_value(needle), f)?;
        Ok(())
    }
    fn inspect(&self, tag: &str, frame: &BufferFrame) {
        self.inspect(tag, U::bf_to_page_value(frame));
    }
//     fn find_parent(&self) {
//         self.find_parent(self.get_page_value())
//     }
}

pub(crate) type DataStructureId = u64;

pub(crate) struct Registry {
    last_id: AtomicU64,
    dt_map: scc::HashMap<DataStructureId, Arc<dyn ErasedDataStructure + Send + Sync>>
}

impl Registry {
    pub(crate) fn new() -> Registry {
        Registry {
            last_id: AtomicU64::new(1),
            dt_map: Default::default()
        }
    }

    pub(crate) fn reserve_dtid(&self) -> DataStructureId {
        self.last_id.fetch_add(1, Ordering::AcqRel)
    }

    pub(crate) fn register(&self, dtid: DataStructureId, dt: Arc<dyn ErasedDataStructure + Send + Sync>) {
        if let Err(e) = self.dt_map.insert(dtid, dt) {
            panic!("failed to register data structure, already exists");
        }
    }

    pub(crate) fn get(&self, dtid: DataStructureId) -> Option<Arc<dyn ErasedDataStructure + Send + Sync>> {
        self.dt_map.read(&dtid, |_, v| v.clone())
    }
}
