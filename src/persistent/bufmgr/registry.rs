use std::{sync::Arc, fmt::Debug};

use once_cell::sync::OnceCell;

use super::{BufferFrame, OptSwipGuard, Swip, swip::Pid, BASE_SIZE_CLASS, BufferManager};
use crate::{error, latch::{HybridLatch, OptimisticGuard, ExclusiveGuard, HybridGuard, SharedGuard}};

pub(crate) enum ParentResult {
    Root,
    Parent(OptSwipGuard<'static>)
}

#[derive(Debug)]
pub(crate) struct FrameDebugInfo {
    pid: Pid,
    value_info: Box<dyn Debug + Send>
}

pub(crate) trait ManagedDataStructure {
    type PageValue;
    fn bf_to_page_value(bf: &BufferFrame) -> &Self::PageValue;
    fn bf_to_page_value_mut(bf: &mut BufferFrame) -> &mut Self::PageValue;
    fn root(&self) -> &'static HybridLatch<BufferFrame>;
    fn find_parent(&self, needle: &impl HybridGuard<Self::PageValue, BufferFrame>) -> error::Result<ParentResult>;
    fn iterate_children_swips<'a>(&self, needle: &Self::PageValue, f: Box<dyn FnMut(&Swip<HybridLatch<BufferFrame>>) -> error::Result<bool> + 'a>) -> error::Result<()>;
    fn iterate_children_swips_mut<'a>(&self, needle: &mut Self::PageValue, f: Box<dyn FnMut(&mut Swip<HybridLatch<BufferFrame>>) -> bool + 'a>);
    fn inspect(&self, _tag: &str, _value: &Self::PageValue) {}
    fn debug_info(&self, _value: &Self::PageValue) -> Box<dyn Debug + Send> {
        Box::new(())
    }
}

pub(crate) trait ErasedDataStructure {
    fn root(&self) -> &'static HybridLatch<BufferFrame>;
    fn find_parent(&self, needle: OptimisticGuard<'static, BufferFrame>) -> error::Result<(ParentResult, OptimisticGuard<'static, BufferFrame>)>;
    fn iterate_children_swips<'a>(&self, needle: &BufferFrame, f: Box<dyn FnMut(&Swip<HybridLatch<BufferFrame>>) -> error::Result<bool> + 'a>) -> error::Result<()>;
    fn iterate_children_swips_mut<'a>(&self, needle: &mut BufferFrame, f: Box<dyn FnMut(&mut Swip<HybridLatch<BufferFrame>>) -> bool + 'a>);
    fn inspect(&self, tag: &str, needle: &BufferFrame);
    fn debug_info(&self, frame: &BufferFrame) -> FrameDebugInfo;
}

impl <T, U: ManagedDataStructure<PageValue = T>> ErasedDataStructure for U {
    fn root(&self) -> &'static HybridLatch<BufferFrame> {
        self.root()
    }
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
    fn iterate_children_swips_mut<'a>(&self, needle: &mut BufferFrame, f: Box<dyn FnMut(&mut Swip<HybridLatch<BufferFrame>>) -> bool + 'a>) {
        self.iterate_children_swips_mut(U::bf_to_page_value_mut(needle), f);
    }
    fn inspect(&self, tag: &str, frame: &BufferFrame) {
        self.inspect(tag, U::bf_to_page_value(frame));
    }
    fn debug_info(&self, frame: &BufferFrame) -> FrameDebugInfo {
        FrameDebugInfo { pid: frame.pid, value_info: self.debug_info(U::bf_to_page_value(frame)) }
    }
}

macro_rules! const_assert {
    ($($tt:tt)*) => {
        const _: () = assert!($($tt)*);
    }
}

pub(crate) fn bf_to_catalog_page(bf: &BufferFrame) -> &CatalogPage {
    unsafe { & *(std::ptr::addr_of!(bf.page.value) as *const CatalogPage) }
}

pub(crate) fn bf_to_catalog_page_mut(bf: &mut BufferFrame) -> &mut CatalogPage {
    unsafe { &mut *(std::ptr::addr_of!(bf.page.value) as *mut CatalogPage) }
}

pub(crate) type DataStructureId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum DataStructureType {
    EMPTY = 0,
    BTREE = 1,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub(super) struct CatalogEntry {
    pub(crate) dtid: DataStructureId,
    pub(crate) dt_type: DataStructureType,
    pub(crate) pid: Pid
}

pub(crate) const CATALOG_DTID: u64 = u64::MAX;
const BASE_PAGE_SIZE: usize = 2usize.pow(BASE_SIZE_CLASS as u32);

const MAX_FREE_PID_ENTRIES: usize = (BASE_PAGE_SIZE / 2) / std::mem::size_of::<u64>();

const MAX_CATALOG_ENTRIES: usize =  (
    BASE_PAGE_SIZE
        - (std::mem::size_of::<u64>() * 3 + std::mem::size_of::<bool>())
        - std::mem::size_of::<u64>() * 2 // next_offset, free_pids_len
        - std::mem::size_of::<[Pid; MAX_FREE_PID_ENTRIES]>() // free_pids
) / std::mem::size_of::<CatalogEntry>();

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub(crate) struct CatalogPage {
    last_id: u64,
    len: u64,
    initialized: bool,
    entries: [CatalogEntry; MAX_CATALOG_ENTRIES],
    // Using fields below to store bufmgr metadata while there is no recovery impl
    next_offset: u64,
    free_pids_len: u64,
    free_pids: [Pid; MAX_FREE_PID_ENTRIES]
}

impl CatalogPage {
    fn get_catalog_entry(&self, dtid: DataStructureId) -> Option<&CatalogEntry> {
        assert!(self.initialized, "catalog page not initialized");
        if self.last_id >= dtid && dtid < self.len && self.entries[dtid as usize].dt_type != DataStructureType::EMPTY {
            Some(&self.entries[dtid as usize])
        } else {
            None
        }
    }

    fn reserve_dtid(&mut self) -> Option<DataStructureId> {
        let dtid = self.last_id;
        if dtid as usize >= MAX_CATALOG_ENTRIES {
            return None;
        }

        self.len += 1;
        self.last_id += 1;
        Some(dtid)
    }

    fn update_catalog_entry(&mut self, entry: CatalogEntry) -> Option<()> {
        if self.last_id >= entry.dtid && entry.dtid < self.len {
            self.entries[entry.dtid as usize] = entry;
            Some(())
        } else {
            None
        }
    }
}

pub(crate) struct Catalog {
    page_latch: OnceCell<&'static HybridLatch<BufferFrame>>,
    dt_map: scc::HashMap<DataStructureId, Arc<dyn ErasedDataStructure + Send + Sync>>
}

impl Catalog {
    pub(crate) fn new() -> Catalog {
        Catalog {
            page_latch: OnceCell::new(),
            dt_map: Default::default()
        }
    }

    pub(crate) fn init(&self, _bufmgr: &'static BufferManager, frame_x: ExclusiveGuard<'static, BufferFrame>) {
        let mut page_x = ExclusiveGuard::map(frame_x, |bf| {
            bf_to_catalog_page_mut(bf)
        });

        if page_x.initialized {
            self.page_latch.set(page_x.latch()).expect("once");
        } else {
            self.page_latch.set(page_x.latch()).expect("once");
            page_x.initialized = true;

            // TODO write page?
        }
    }

    pub(super) fn page_latch(&self) -> &'static HybridLatch<BufferFrame> {
        self.page_latch.get().expect("catalog not initialized")
    }

    fn page_shared(&self) -> SharedGuard<CatalogPage, BufferFrame> {
        let frame_shr = self.page_latch.get().expect("catalog not initialized").shared();
        let page_shr = SharedGuard::map(frame_shr, |bf| {
            bf_to_catalog_page(bf)
        });

        page_shr
    }

    fn page_exclusive(&self) -> ExclusiveGuard<CatalogPage, BufferFrame> {
        let frame_exv = self.page_latch.get().expect("catalog not initialized").exclusive();
        let page_exv = ExclusiveGuard::map(frame_exv, |bf| {
            bf_to_catalog_page_mut(bf)
        });

        page_exv
    }

    pub(super) fn next_offset(&self) -> u64 {
        self.page_shared().next_offset
    }

    pub(super) fn free_pids(&self) -> Vec<Pid> {
        let page = self.page_shared();
        page.free_pids[..(page.free_pids_len as usize)].to_vec()
    }

    pub(super) fn store_next_offset(&self, next_offset: u64) {
        self.page_exclusive().next_offset = next_offset;
    }

    pub(super) fn store_free_pids(&self, free_pids: Vec<Pid>) {
        // TODO warn about pid leaks here
        let len = free_pids.len().min(MAX_FREE_PID_ENTRIES);
        let mut page = self.page_exclusive();
        page.free_pids[..len].copy_from_slice(&free_pids[..len]);
        page.free_pids_len = len as u64;
    }

    pub(super) fn get_catalog_entry(&self, dtid: DataStructureId) -> Option<CatalogEntry> {
        self.page_shared().get_catalog_entry(dtid).map(|e| *e)
    }

    pub(super) fn update_catalog_entry(&self, entry: CatalogEntry) -> Option<()> {
        self.page_exclusive().update_catalog_entry(entry)
    }

    pub(crate) fn reserve_dtid(&self) -> Option<DataStructureId> {
        self.page_exclusive().reserve_dtid()
    }

    pub(crate) fn get(&self, dtid: DataStructureId) -> Option<Arc<dyn ErasedDataStructure + Send + Sync>> {
        self.dt_map.read(&dtid, |_, v| v.clone())
    }

    pub(crate) fn register(&self, dtid: DataStructureId, dt: Arc<dyn ErasedDataStructure + Send + Sync>) {
        if let Err(e) = self.dt_map.insert(dtid, dt) {
            panic!("failed to register data structure, already exists");
        }
    }

    pub(crate) fn remove(&self, dtid: DataStructureId) -> Option<Arc<dyn ErasedDataStructure + Send + Sync>> {
        self.dt_map.remove(&dtid).map(|t| t.1)
    }
}

const_assert!(std::mem::size_of::<CatalogPage>() <= 2usize.pow(BASE_SIZE_CLASS as u32));
