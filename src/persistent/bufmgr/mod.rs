use nix::sys::mman::{
    ProtFlags,
    MapFlags,
    MmapAdvise
};

use crossbeam_queue::{ArrayQueue, SegQueue};

use parking_lot::Mutex;
use rand::seq::SliceRandom;

use crate::persistent::bufmgr::registry::ErasedDataStructure;
use crate::latch::{HybridLatch, OptimisticGuard, SharedGuard, ExclusiveGuard, HybridGuard};

use crate::error::{self, BufMgrError};

use std::fmt;
use std::sync::{
    atomic::{AtomicUsize, AtomicBool, Ordering},
    Arc
};
use std::collections::HashMap;

pub mod swip;

pub mod registry;

pub mod latch_ext;

pub mod write_buffer;

pub mod page_provider;

use write_buffer::WriteBuffer;

use swip::{Pid, Swip, RefOrPid};

use registry::{DataStructureId, ParentResult, Catalog};

use self::registry::{CATALOG_DTID, DataStructureType, ManagedDataStructure, CatalogEntry};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Page {
    size_class: u64,
    gsn: u64,
    dtid: DataStructureId,
    pub(crate) value: () // Value starts at `addr_of(self.value)` and ends at `addr_of(self.value) + ((size_of_class_in_bytes) - size_of(Page))`
}

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
#[repr(C)]
pub enum BfState {
    Free,
    Hot,
    Loaded,
    Reclaim
}

pub(crate) const EMPTY_EPOCH: usize = usize::MAX;
pub(crate) const RESERVED_EPOCH: usize = usize::MAX - 1;

#[derive(Debug)]
#[repr(C)]
pub struct BufferFrame {
    pub state: BfState,
    pub pid: Pid,
    pub last_written_gsn: u64,
    pub writting: bool,
    pub persisting: AtomicBool,
    pub epoch: AtomicUsize,
    pub(crate) page: &'static mut Page
}

impl BufferFrame {
    fn reset(&mut self) {
        assert!(!self.writting);
        let old_pid = self.pid;
        let size = 2usize.pow(old_pid.size_class() as u32);
        self.last_written_gsn = 0;
        self.writting = false;
        self.state = BfState::Free;
        self.pid = Pid::new_invalid(old_pid.size_class());

        self.persisting = AtomicBool::new(false);
        self.epoch = AtomicUsize::new(EMPTY_EPOCH);
        #[cfg(debug_assertions)]
        {
            self.page_bytes_mut().fill(0);
        }
    }

    fn is_dirty(&self) -> bool {
        self.page.gsn > self.last_written_gsn
    }

    fn page_bytes(&self) -> &[u8] {
        let size = 2usize.pow(self.pid.size_class() as u32);
        unsafe { std::slice::from_raw_parts(self.page as *const _ as *const u8, size) }
    }

    fn page_bytes_mut(&mut self) -> &mut [u8] {
        let size = 2usize.pow(self.pid.size_class() as u32);
        unsafe { std::slice::from_raw_parts_mut(self.page as *mut _ as *mut u8, size) }
    }
}

unsafe impl Sync for BufferFrame {}
unsafe impl Send for BufferFrame {}

// TODO Limit pool size across size classes
#[derive(Debug)]
pub struct SizeClass {
    class: usize,
    offset: Arc<AtomicUsize>,
    frames: Vec<HybridLatch<BufferFrame>>,
    free_frames: ArrayQueue<&'static HybridLatch<BufferFrame>>,
    free_pids: SegQueue<Pid>,
    cool_frames: ArrayQueue<&'static HybridLatch<BufferFrame>>,
}

impl SizeClass {
    pub fn class_size(&self) -> usize {
        2usize.pow(self.class as u32)
    }

    pub fn contains_frame(&self, frame: &HybridLatch<BufferFrame>) -> bool {
        self.frames.iter().find(|&f| f as *const _ == frame as *const _).is_some()
    }

    pub fn next_pid(&self) -> Pid {
        if let Some(pid) = self.free_pids.pop() {
            pid
        } else {
            let reserved_offset = self.offset.fetch_add(self.class_size(), Ordering::AcqRel);
            Pid::new(reserved_offset as u64, self.class as u8)
        }
    }

    pub fn free_page(&self, pid: Pid) {
        self.free_pids.push(pid);
    }

    pub fn allocate_page(&'static self, dtid: DataStructureId, epoch: usize) -> Result<ExclusiveGuard<'static, BufferFrame>, BufMgrError> {
        let free_bf = self.free_frames.pop().ok_or_else(|| BufMgrError::OutOfFrames(self.class))?;

        let free_pid = self.next_pid();

        debug_assert!(!free_bf.is_exclusively_latched());
        let mut bf = free_bf.exclusive();
        assert!(bf.state == BfState::Free);
        bf.pid = free_pid;
        bf.state = BfState::Hot;
        bf.last_written_gsn = 0;
        bf.page.size_class = self.class as u64;
        bf.page.gsn = 1; // Starting page gsn as 1 to ensure it will be written at least once
        bf.page.dtid = dtid;
        bf.epoch.store(epoch, Ordering::Release);

        // if free_pid.page_id() > pool_size { println!("going larger than memory") }

        Ok(bf)
    }

    pub fn reclaim_page(&self, mut frame: ExclusiveGuard<'static, BufferFrame>) {
        assert_eq!(frame.page.size_class, self.class as u64);

        if frame.writting || frame.persisting.load(Ordering::Acquire) {
            frame.state = BfState::Reclaim;
            let _ = frame.unlock();
            // frame will be pushed into free_frames after writting is complete
        } else {
            self.free_page(frame.pid);

            frame.reset();
            let unlocked = frame.unlock();
            if let Err(e) = self.free_frames.push(unlocked.latch()) {
                unreachable!("should have space");
            }
        }
    }

    pub fn random_frame(&'static self) -> &'static HybridLatch<BufferFrame> {
        self.frames.choose(&mut rand::thread_rng()).expect("must have at least one frame")
    }
}

enum IoCommand {
    Load,
    Swizzle(ExvSwipGuard<'static>),
    Evict(ExclusiveGuard<'static, BufferFrame>)
}

enum IoOutcome {
    Ok,
    SwipAndFrame(ExvSwipGuard<'static>, &'static HybridLatch<BufferFrame>),
    Noop
}

#[derive(Debug)]
enum IoState {
    Evicted,
    Loaded(&'static HybridLatch<BufferFrame>),
    Swizzled(&'static HybridLatch<BufferFrame>),
    Aborted
}

#[derive(Debug)]
struct IoSlot {
    bufmgr: &'static BufferManager,
    state: IoState,
    pid: Pid
}

impl IoSlot {
    fn new_evicted(bufmgr: &'static BufferManager, pid: Pid) -> Self {
        IoSlot {
            bufmgr,
            state: IoState::Evicted,
            pid
        }
    }

    fn new_swizzled(bufmgr: &'static BufferManager, pid: Pid, latched_frame: &'static HybridLatch<BufferFrame>) -> Self {
        IoSlot {
            bufmgr,
            state: IoState::Swizzled(latched_frame),
            pid
        }
    }

    fn transition(&mut self, command: IoCommand) -> Result<IoOutcome, BufMgrError> {
        use IoState::*;
        use IoCommand::*;

        match (&self.state, command)  {
            (Evicted | Aborted, Load) => {
                let size_class = self.bufmgr.size_class(self.pid.size_class());
                let free_bf = size_class.free_frames.pop().ok_or_else(|| BufMgrError::OutOfFrames(size_class.class))?;

                let mut frame = free_bf.exclusive();

                assert!(frame.state == BfState::Free);

                self.bufmgr.read_page_sync(self.pid, frame.page).map_err(|e| { println!("failed to read: {}", e); e })?;
                frame.last_written_gsn = frame.page.gsn;
                frame.state = BfState::Loaded;
                assert!(!frame.writting);
                frame.pid = self.pid;
                frame.epoch.store(self.bufmgr.global_epoch.load(Ordering::Acquire), Ordering::Release);

                self.state = Loaded(free_bf);
                Ok(IoOutcome::Ok)
            }
            (Loaded(loaded_frame), Swizzle(mut swip_x_guard)) => {
                let mut frame = loaded_frame.exclusive();
                assert!(!swip_x_guard.is_ref());
                if let RefOrPid::Pid(p) = swip_x_guard.downcast() {
                    assert_eq!(self.pid, p);
                    assert_eq!(self.pid, frame.pid);
                } else {
                    panic!("not pid");
                }
                swip_x_guard.to_ref(loaded_frame);
                frame.state = BfState::Hot;

                crate::dbg_swizzle_in!(self.bufmgr, swip_x_guard, &frame);

                let outcome = IoOutcome::SwipAndFrame(swip_x_guard, loaded_frame);
                self.state = Swizzled(loaded_frame);
                Ok(outcome)
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

pub struct BufferManager {
    fd: std::fs::File,
    pool_size: usize,
    offset: Arc<AtomicUsize>,
    classes: Vec<SizeClass>,
    io_map: Mutex<HashMap<Pid, Arc<Mutex<IoSlot>>>>,
    catalog: Catalog,
    running: AtomicBool,
    global_epoch: AtomicUsize,
}

impl fmt::Debug for BufferManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct IoMapPlaceholder;
        impl fmt::Debug for IoMapPlaceholder {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("<map>")
            }
        }
        f.debug_struct("BufferManager")
            .field("fd", &self.fd)
            .field("offset", &self.offset)
            .field("classes", &self.classes)
            .field("io_map", &IoMapPlaceholder)
            .finish()
    }
}

const MAX_SIZE_CLASSES: usize = 32;
pub (crate) const BASE_SIZE_CLASS: usize = 16; // 2 ^ 16 == 64k

impl BufferManager {
    pub fn new(fd: std::fs::File, pool_size: usize) -> Self {
        let offset = Arc::new(AtomicUsize::new(0));

        let mut classes = vec!();
        for i in 0..MAX_SIZE_CLASSES {
            let class = BASE_SIZE_CLASS + i;
            if let Some(class_size) = 2usize.checked_pow(class as u32) {
                let n_frames = if class_size <= pool_size {
                    pool_size / class_size
                } else {
                    0
                };

                println!("2 ^ {}, frames: {:?}", class, n_frames);

                if n_frames > 0 {
                    let mut frames = vec!();
                    let addr = unsafe {
                        nix::sys::mman::mmap(
                            std::ptr::null_mut(),
                            pool_size,
                            ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                            MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS,
                            -1,
                            0
                        ).expect("failed to init buffer manager pool")
                    };

                    unsafe {
                        nix::sys::mman::madvise(
                            addr,
                            pool_size,
                            MmapAdvise::MADV_DONTFORK
                        ).expect("failed to configure pool")
                    };

                    assert!((addr as usize) % 4096 == 0);

                    for frame_idx in 0..n_frames {
                        let ptr = unsafe { (addr as *mut u8).add(frame_idx * class_size) } as *mut Page;

                        let page_ref: &'static mut Page = unsafe { &mut *ptr };

                        page_ref.size_class = class as u64;

                        let latched_frame = HybridLatch::new(BufferFrame {
                            state: BfState::Free,
                            pid: Pid::default(),
                            last_written_gsn: 0,
                            writting: false,
                            persisting: AtomicBool::new(false),
                            epoch: AtomicUsize::new(EMPTY_EPOCH),
                            page: page_ref
                        });

                        frames.push(latched_frame);
                    }

                    classes.push(SizeClass {
                        class,
                        offset: Arc::clone(&offset),
                        free_frames: ArrayQueue::new(frames.len()),
                        free_pids: SegQueue::new(),
                        cool_frames: ArrayQueue::new(frames.len()),
                        frames
                    });
                }
            }
        }

        BufferManager {
            fd,
            pool_size,
            offset,
            classes,
            io_map: Mutex::new(HashMap::default()),
            catalog: Catalog::new(),
            running: AtomicBool::new(false),
            global_epoch: AtomicUsize::new(0),
        }
    }

    pub fn init(&'static self) {
        for (class_idx, size_class) in self.classes.iter().enumerate() {
            let mut frames: Vec<_> = if class_idx == 0 {
                // Reserving first frame for catalog
                size_class.frames.iter().skip(1).collect()
            } else {
                size_class.frames.iter().collect()
            };
            frames.shuffle(&mut rand::thread_rng());
            for frame in frames {
                if let Err(e) = size_class.free_frames.push(frame) {
                    unreachable!("should have space");
                }
            }
        }

        let base_class = self.classes.first().expect("must exist");
        let catalog_frame_latch = base_class.frames.first().expect("base class has zero frames");
        let mut catalog_frame_x = catalog_frame_latch.exclusive();

        let catalog_pid = base_class.next_pid(); // FIXME ensure this is the first pid

        let read_bytes = self.try_read_page_sync(catalog_pid, catalog_frame_x.page).expect("failed to read catalog page");
        if read_bytes == 0 {
            // Uninitialized
            catalog_frame_x.pid = catalog_pid;
            catalog_frame_x.state = BfState::Hot;
            catalog_frame_x.last_written_gsn = 0;
            catalog_frame_x.page.size_class = base_class.class as u64;
            catalog_frame_x.page.gsn = 1; // Starting page gsn as 1 to ensure it will be written at least once
            catalog_frame_x.page.dtid = CATALOG_DTID;
            catalog_frame_x.epoch.store(RESERVED_EPOCH, Ordering::Release);

            // Init catalog
            self.catalog.init(self, catalog_frame_x);

            self.catalog.store_next_offset(self.offset.load(Ordering::Acquire) as u64);
        } else if read_bytes < base_class.class_size() {
            panic!("could not read the catalog page completely, may have been written partialy");
        } else {
            // Initialized

            // TODO crc check

            catalog_frame_x.pid = catalog_pid;
            catalog_frame_x.state = BfState::Hot;
            catalog_frame_x.last_written_gsn = catalog_frame_x.page.gsn;
            assert_eq!(catalog_frame_x.page.size_class, base_class.class as u64, "wrong catalog size_class");
            assert_eq!(catalog_frame_x.page.dtid, CATALOG_DTID, "wrong catalog dtid");
            catalog_frame_x.epoch.store(RESERVED_EPOCH, Ordering::Release);

            // Ensure catalog initialized
            self.catalog.init(self, catalog_frame_x);

            // Loading offset
            let next_offset = self.catalog.next_offset();

            assert_ne!(0, next_offset);

            self.offset.store(next_offset as usize, Ordering::Release);
        }

        let free_pids = self.catalog.free_pids();

        for pid in free_pids {
            let size_class = self.size_class(pid.size_class());
            size_class.free_pids.push(pid);
        }

        self.running.store(true, Ordering::Release);
        let _ = std::thread::spawn(move || { // TODO keep handle
            let ids = core_affinity::get_core_ids().expect("has core_ids");
            let id = ids.into_iter().nth(0).expect("must have one");
            if !core_affinity::set_for_current(id) {
                panic!("failed to set affinity");
            }
            self.page_provider_epoch()
        });
    }

    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn read_page_sync(&self, pid: Pid, page: &mut Page) -> Result<(), BufMgrError>{
        use nix::sys::uio::pread;
        use std::os::unix::io::AsRawFd;

        let size_class = self.size_class(pid.size_class());
        let size = size_class.class_size();
        let offset = pid.page_id();
        let mut slice = unsafe { std::slice::from_raw_parts_mut(page as *mut _ as *mut u8, size) };
        let n = pread(self.fd.as_raw_fd(), &mut slice, offset as i64).map_err(|e| BufMgrError::Io(e))?;
        if n != size {
            println!("BUG pid: {:?}, slice_len: {}, offset: {}, n: {}", pid, slice.len(), offset, n);
        }
        assert_eq!(n, size);
        Ok(())
    }

    pub fn try_read_page_sync(&self, pid: Pid, page: &mut Page) -> Result<usize, BufMgrError>{
        use nix::sys::uio::pread;
        use std::os::unix::io::AsRawFd;

        let size_class = self.size_class(pid.size_class());
        let size = size_class.class_size();
        let offset = pid.page_id();
        let mut slice = unsafe { std::slice::from_raw_parts_mut(page as *mut _ as *mut u8, size) };
        let n = pread(self.fd.as_raw_fd(), &mut slice, offset as i64).map_err(|e| BufMgrError::Io(e))?;
        Ok(n)
    }

    pub fn write_page_sync(&self, pid: Pid, page: &Page) -> Result<(), BufMgrError>{
        use nix::sys::uio::pwrite;
        use std::os::unix::io::AsRawFd;

        let size_class = self.size_class(pid.size_class());
        let size = size_class.class_size();
        let offset = pid.page_id();
        let mut slice = unsafe { std::slice::from_raw_parts(page as *const _ as *const u8, size) };
        let n = pwrite(self.fd.as_raw_fd(), &slice, offset as i64).map_err(|e| BufMgrError::Io(e))?;
        assert!(n == size);
        Ok(())
    }

    #[inline]
    pub fn size_class<U>(&self, size_class: U) -> &SizeClass
    where
        U: std::convert::TryInto<u64>,
        U::Error: std::fmt::Debug
    {
        let size_class = size_class.try_into().expect("failed to convert size class");
        &self.classes[size_class as usize - BASE_SIZE_CLASS]
    }

    pub(crate) fn capacity_for<T>(size: usize) -> usize {
        let overhead = std::mem::size_of::<Page>() + std::mem::size_of::<T>();
        2usize.pow(BASE_SIZE_CLASS as u32).max((size + overhead).next_power_of_two())
    }

    pub fn allocate_page_for<T>(&'static self, dtid: DataStructureId, size: usize) -> Result<(ExclusiveGuard<'static, BufferFrame>, usize), BufMgrError> {
        let overhead = std::mem::size_of::<Page>() + std::mem::size_of::<T>();
        let class = BASE_SIZE_CLASS.max(((size + overhead).next_power_of_two() as f64).log2().floor() as usize);
        let size_class = self.size_class(class);
        Ok((size_class.allocate_page(dtid, self.global_epoch.load(Ordering::Acquire))?, size_class.class_size() - overhead))
    }

    pub fn reclaim_page(&self, mut frame: ExclusiveGuard<'static, BufferFrame>) {
        self.size_class(frame.page.size_class).reclaim_page(frame);
    }

    pub fn write_all_buffer_frames(&self) {
        for size_class in self.classes.iter() {
            for latch in size_class.frames.iter() {
                let frame = latch.shared();
                if frame.state == BfState::Hot {
                    use nix::sys::uio::pwrite;
                    use std::os::unix::io::AsRawFd;

                    let size = size_class.class_size();
                    let offset = frame.pid.page_id();
                    let slice = unsafe { std::slice::from_raw_parts(frame.page as *const _ as *const u8, size) };
                    let n = pwrite(self.fd.as_raw_fd(), slice, offset as i64).expect("failed to write");
                    assert!(n == size);
                }
            }
        }
    }

    pub(crate) fn create_data_structure<T, F>(&'static self, dt_type: DataStructureType, f: F) -> Option<(DataStructureId, Arc<T>)>
    where
        T: ManagedDataStructure + Send + Sync + 'static,
        F: FnOnce(DataStructureId, ExclusiveGuard<'static, T::PageValue, BufferFrame>, usize) -> T
    {
        let dtid = self.catalog().reserve_dtid()?;

        let (guard, capacity) = match self.allocate_page_for::<T::PageValue>(dtid, 1) {
            Ok((bf_guard, capacity)) => {
                let value_guard = ExclusiveGuard::map(bf_guard, |bf| {
                    T::bf_to_page_value_mut(bf)
                });
                (value_guard, capacity)
            }
            Err(BufMgrError::OutOfFrames(_)) => {
                panic!("no more frames while allocating root");
            }
            _ => {
                panic!("failed to allocate root");
            }
        };

        let pid = guard.as_unmapped().pid;

        let dt = Arc::new(f(dtid, guard, capacity));

        self.catalog().update_catalog_entry(CatalogEntry {
            dtid,
            dt_type,
            pid
        })?;

        self.catalog().register(dtid, dt.clone());

        Some((dtid, dt))
    }

    pub fn load_data_structure_root(&self, dtid: DataStructureId, dt_type: DataStructureType) -> Option<ExclusiveGuard<'static, BufferFrame>> {
        let entry = self.catalog().get_catalog_entry(dtid)?;

        if entry.dt_type != dt_type {
            return None;
        }

        assert_eq!(dtid, entry.dtid);
        let size_class = self.size_class(entry.pid.size_class());
        let free_bf = size_class.free_frames.pop()?;

        let mut frame = free_bf.exclusive();

        assert!(frame.state == BfState::Free);

        self.read_page_sync(entry.pid, frame.page).ok()?;
        frame.last_written_gsn = frame.page.gsn;
        frame.state = BfState::Hot;
        assert!(!frame.writting);
        frame.pid = entry.pid;
        frame.epoch.store(RESERVED_EPOCH, Ordering::Release); // TODO check that it can be manually
                                                              // reclaimed in reclaim_page
        Some(frame)
    }

    pub(crate) fn load_data_structure<T, F>(&self, dtid: DataStructureId, dt_type: DataStructureType, f: F) -> Option<Arc<T>>
    where
        T: ManagedDataStructure + Send + Sync + 'static,
        F: FnOnce(DataStructureId, ExclusiveGuard<'static, T::PageValue, BufferFrame>) -> T
    {
        if self.catalog().get(dtid).is_some() {
            // Already loaded
            // TODO better return type
            return None;
        }

        let root_bf = self.load_data_structure_root(dtid, dt_type)?;
        let value = ExclusiveGuard::map(root_bf, |bf| {
            T::bf_to_page_value_mut(bf)
        });

        let dt = Arc::new(f(dtid, value));

        self.catalog().register(dtid, dt.clone());

        Some(dt)
    }

    pub(crate) fn evict_data_structure<T: ErasedDataStructure + Send + Sync>(&self, dtid: DataStructureId, dt_type: DataStructureType, dt: Arc<T>) -> Result<(), Arc<T>> {
        if Arc::strong_count(&dt) > 2 { // 2 = here and in catalog
            return Err(dt);
        }

        let pid = self.evict_data_structure_root(dtid).ok_or(dt)?;

        self.catalog().update_catalog_entry(CatalogEntry {
            dtid,
            dt_type,
            pid
        }).expect("cannot update neither return the datastructure");

        let _ = self.catalog().remove(dtid).expect("exists");

        Ok(())
    }

    pub fn evict_data_structure_root(&self, dtid: DataStructureId) -> Option<Pid> {
        let dt = self.catalog().get(dtid)?;
        let root = dt.root();


        fn evict_frame(bufmgr: &BufferManager, dt: &Arc<dyn ErasedDataStructure + Send + Sync>, swip: &mut Swip<HybridLatch<BufferFrame>>) {
            match swip.downcast() {
                RefOrPid::Ref(latch) => {
                    let mut frame_x = latch.exclusive();

                    dt.iterate_children_swips_mut(&mut frame_x, Box::new(|swip| {
                        evict_frame(bufmgr, dt, swip);
                        true
                    }));

                    let size_class = bufmgr.size_class(frame_x.page.size_class);

                    bufmgr.write_page_sync(frame_x.pid, frame_x.page).expect("failed to write page");

                    swip.to_pid(frame_x.pid);

                    frame_x.reset();

                    let unlocked = frame_x.unlock();
                    if let Err(e) = size_class.free_frames.push(unlocked.latch()) {
                        unreachable!("should have space");
                    }
                }
                RefOrPid::Pid(_) => ()
            }
        }

        let mut root_swip = Swip::from_ref(root);

        evict_frame(self, &dt, &mut root_swip);

        assert!(root_swip.is_pid());

        if let RefOrPid::Pid(pid) = root_swip.downcast() {
            Some(pid)
        } else {
            unreachable!("must have evicted");
        }
    }

    // FIXME persisting this does not ensure durability unless this is the last operation before
    // shutdown
    pub fn persist_metadata(&self) -> Result<(), BufMgrError> {
        self.catalog.store_next_offset(self.offset.load(Ordering::Acquire) as u64);
        let mut free_pids = vec![];
        for size_class in self.classes.iter() {
            while let Some(pid) = size_class.free_pids.pop() {
                free_pids.push(pid);
            }
        }

        self.catalog.store_free_pids(free_pids);

        let latch = self.catalog.page_latch();

        let frame_x = latch.shared();

        let size_class = self.size_class(frame_x.page.size_class);

        self.write_page_sync(frame_x.pid, frame_x.page)?;

        Ok(())
    }
}

pub type OptSwipGuard<'a> = OptimisticGuard<'a, Swip<HybridLatch<BufferFrame>>, BufferFrame>;
pub type ShrSwipGuard<'a> = SharedGuard<'a, Swip<HybridLatch<BufferFrame>>, BufferFrame>;
pub type ExvSwipGuard<'a> = ExclusiveGuard<'a, Swip<HybridLatch<BufferFrame>>, BufferFrame>;

#[inline]
pub(crate) fn sync_frame_epoch(latch: &'static HybridLatch<BufferFrame>, epoch: usize) {
    let frame_epoch_atomic = unsafe { &(*latch.data_ptr()).epoch };
    let frame_epoch = frame_epoch_atomic.load(Ordering::Acquire);
    if frame_epoch < epoch {
        frame_epoch_atomic.store(epoch, Ordering::Release);
    }
}

impl BufferManager {
    #[inline]
    pub fn resolve_swip_fast(
        &'static self,
        swip_guard: OptSwipGuard<'static>,
        spin: bool,
    ) -> error::Result<(
        OptSwipGuard<'static>,
        &'static HybridLatch<BufferFrame>
    )> {
        match swip_guard.downcast() {
            RefOrPid::Ref(r) => {
                swip_guard.recheck()?;
                sync_frame_epoch(r, self.global_epoch.load(Ordering::Acquire));
                Ok((swip_guard, r))
            },
            RefOrPid::Pid(_) => {
                self.resolve_swip(swip_guard, spin)
            }
        }
    }

    pub fn resolve_swip(
        &'static self,
        mut swip_guard: OptSwipGuard<'static>,
        spin: bool,
    ) -> error::Result<(
        OptSwipGuard<'static>,
        &'static HybridLatch<BufferFrame>
    )> {
        match swip_guard.downcast() {
            RefOrPid::Ref(r) => {
                swip_guard.recheck()?;
                sync_frame_epoch(r, self.global_epoch.load(Ordering::Acquire));
                return Ok((swip_guard, r));
            },
            RefOrPid::Pid(pid) => {

                swip_guard.recheck()?;

                // Get locked slot

                return self.with_slot_or_create(pid, IoSlot::new_evicted(self, pid), |slot_guard, returned| {
                    match &slot_guard.state {
                        IoState::Evicted | IoState::Aborted => {
                            if let Err(err @ error::Error::Unwind) = swip_guard.recheck() {
                                slot_guard.state = IoState::Aborted;
                                return (true, Err(err));
                            }

                            match slot_guard.transition(IoCommand::Load) {
                                Err(BufMgrError::OutOfFrames(_)) => {
                                    slot_guard.state = IoState::Aborted;
                                    return (true, Err(error::Error::Unwind)); // Retry
                                }
                                Err(err) => {
                                    panic!("failed to load");
                                }
                                _ => {}
                            }

                            let swip_x_guard = match swip_guard.to_exclusive() {
                                Ok(guard) => guard,
                                Err(_) => {
                                    return (false, Err(error::Error::Unwind)); // Retry without cleanup, this can leak a loaded frame
                                }
                            };
                            if let IoOutcome::SwipAndFrame(swip_x_guard, frame_ref) = slot_guard.transition(IoCommand::Swizzle(swip_x_guard)).expect("failed to swizzle") {
                                let swip_guard = swip_x_guard.unlock();
                                sync_frame_epoch(frame_ref, self.global_epoch.load(Ordering::Acquire));
                                return (true, Ok((swip_guard, frame_ref)));
                            } else {
                                unreachable!("no other outcome expected");
                            }
                        }
                        IoState::Loaded(loaded_frame) => {
                            let swip_x_guard = match swip_guard.to_exclusive() {
                                Ok(guard) => guard,
                                Err(_) => {
                                    return (false, Err(error::Error::Unwind)); // Retry without cleanup, this can leak a loaded frame
                                }
                            };

                            if let IoOutcome::SwipAndFrame(swip_x_guard, frame_ref) = slot_guard.transition(IoCommand::Swizzle(swip_x_guard)).expect("failed to swizzle") {
                                let swip_guard = swip_x_guard.unlock();
                                sync_frame_epoch(frame_ref, self.global_epoch.load(Ordering::Acquire));
                                return (true, Ok((swip_guard, frame_ref)));
                            } else {
                                unreachable!("no other outcome expected");
                            }
                        }
                        IoState::Swizzled(swizzled_frame) => {
                            return (true, Err(error::Error::Unwind));
                        }
                    }
                });
            }
        };
    }

    fn with_slot_or_create<F, R>(&'static self, pid: Pid, create_slot: IoSlot, f: F) -> R
        where
            F: FnOnce(&mut IoSlot, Option<IoSlot>) -> (bool, R)
    {
        let mut slot = Arc::new(Mutex::new(create_slot));
        let mut unmoved_slot = Arc::clone(&slot);
        let mut create_guard = unmoved_slot.lock();
        let mut existed = false;
        let mut returned = None;

        let mut map = self.io_map.lock();
        if let Some(slot_ref) = map.get(&pid) {
            slot = Arc::clone(slot_ref);
            existed = true;
        } else {
            assert!(map.insert(pid, Arc::clone(&slot)).is_none());
        }
        drop(map);
        let mut guard = if existed {
            drop(create_guard);
            returned = Some(Arc::try_unwrap(unmoved_slot).expect("must be the only ref").into_inner());
            slot.lock()
        } else {
            create_guard
        };

        let (cleanup, result) = f(&mut guard, returned);

        if cleanup {
            let n_guards = if existed { 2 } else { 3 };
            if Arc::strong_count(&slot) <= n_guards {
                // Try to remove
                let mut map = self.io_map.lock();
                let slot_ref = map.get(&guard.pid).expect("must exist");
                if Arc::strong_count(slot_ref) <= n_guards {
                    map.remove(&guard.pid);
                } else {
                }
            } else {
            }
        }

        return result;
    }

    fn try_with_slot_or_create<F, R>(&'static self, pid: Pid, create_slot: IoSlot, f: F) -> Option<R>
        where
            F: FnOnce(&mut IoSlot, Option<IoSlot>) -> (bool, R)
    {
        let mut slot = Arc::new(Mutex::new(create_slot));
        let mut unmoved_slot = Arc::clone(&slot);
        let mut create_guard = unmoved_slot.lock();
        let mut existed = false;
        let mut returned = None;

        let mut map = self.io_map.lock();
        if let Some(slot_ref) = map.get(&pid) {
            slot = Arc::clone(slot_ref);
            existed = true;
        } else {
            assert!(map.insert(pid, Arc::clone(&slot)).is_none());
        }
        drop(map);
        let mut guard = if existed {
            drop(create_guard);
            returned = Some(Arc::try_unwrap(unmoved_slot).expect("must be the only ref").into_inner());
            match slot.try_lock() {
                Some(guard) => guard,
                None => {
                    return None;
                }
            }
        } else {
            create_guard
        };

        let (cleanup, result) = f(&mut guard, returned);

        if cleanup {
            if Arc::strong_count(&slot) <= 2 {
                // Try to remove
                let mut map = self.io_map.lock();
                let slot_ref = map.get(&guard.pid).expect("must exist");
                if Arc::strong_count(slot_ref) <= 2 {
                    map.remove(&guard.pid);
                }
            }
        }

        return Some(result);
    }

    fn try_with_slot<F, R>(&'static self, pid: Pid, f: F) -> Option<R>
        where
            F: FnOnce(&mut IoSlot) -> (bool, R)
    {
        let mut map = self.io_map.lock();
        let mut slot = if let Some(slot_ref) = map.get(&pid) {
            Arc::clone(slot_ref)
        } else {
            return None; // Slot for pid not found
        };
        drop(map);

        let mut guard = match slot.try_lock() {
            Some(guard) => guard,
            None => {
                return None; // Slot for pid is locked
            }
        };

        let (cleanup, result) = f(&mut guard);

        if cleanup {
            if Arc::strong_count(&slot) <= 2 {
                // Try to remove
                let mut map = self.io_map.lock();
                let slot_ref = map.get(&guard.pid).expect("must exist");
                if Arc::strong_count(slot_ref) <= 2 {
                    map.remove(&guard.pid);
                }
            }
        }

        return Some(result);
    }
}
