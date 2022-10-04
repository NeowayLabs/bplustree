use nix::sys::mman::{
    ProtFlags,
    MapFlags,
    MmapAdvise
};

use crossbeam_queue::ArrayQueue;

use parking_lot::Mutex;

use bplustree::latch::{HybridLatch, OptimisticGuard, SharedGuard, ExclusiveGuard};

use bplustree::error::{self, BufMgrError};

use std::fmt;
use std::sync::{
    atomic::{AtomicUsize, AtomicBool, Ordering},
    Arc
};
use std::collections::HashMap;

pub mod swip;

pub mod registry;

// pub mod write_buffer;

// use write_buffer::WriteBuffer;

use swip::{Pid, Swip, RefOrPid};

use registry::{DataStructureId, Registry, ParentResult};

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
    Cool,
    Loaded
}

#[derive(Debug)]
#[repr(C)]
pub struct BufferFrame {
    pub state: BfState,
    pub pid: Pid,
    // padding: [u8; 512 - (std::mem::size_of::<HybridLatch<()>>() + (std::mem::size_of::<u64>() * 2))], // Alignment hack, very dependent on BufferFrame fields
    pub(crate) page: &'static mut Page
}

impl BufferFrame {
    fn reset(&mut self) {
        let old_pid = self.pid;
        let size = 2usize.pow(old_pid.size_class() as u32);
        // self.last_written_gsn = 0;
        self.state = BfState::Free;
        self.pid = Pid::new_invalid(old_pid.size_class());
        // TODO clear page?
        // unsafe { std::slice::from_raw_parts_mut(self.page as *mut _ as *mut u8, size) }.fill(0);
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

// #[repr(C)]
// pub struct Node {
//     capacity: u64,
//     len: u64,
//     swip: Swip<HybridLatch<BufferFrame<Node>>>,
//     data: () // Data starts at `addr_of(self.data)` and ends at `addr_of(self.data) + self.capacity`
// }
//
// impl Node {
//     #[inline]
//     fn data(&self) -> &[u8] {
//         unsafe { std::slice::from_raw_parts(std::ptr::addr_of!(self.data) as *const u8, self.capacity as usize) }
//     }
//
//     #[inline]
//     fn data_mut(&mut self) -> &mut [u8] {
//         unsafe { std::slice::from_raw_parts_mut(std::ptr::addr_of_mut!(self.data) as *mut u8, self.capacity as usize) }
//     }
// }
//
// impl fmt::Debug for Node {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("Node")
//             .field("capacity", &self.capacity)
//             .field("len", &self.len)
//             .field("swip", &self.swip.as_raw())
//             .field("data", &self.data())
//             .finish()
//     }
// }
//
// pub trait PageValue {
//     type SwipTag;
//
//     fn swip(&self, tag: Self::SwipTag) -> Option<&Swip<HybridLatch<BufferFrame<Self>>>> where Self: Sized;
//
//     fn swip_mut(&mut self, tag: Self::SwipTag) -> Option<&mut Swip<HybridLatch<BufferFrame<Self>>>> where Self: Sized;
// }

#[derive(Debug)]
pub struct SizeClass {
    class: usize,
    offset: Arc<AtomicUsize>,
    frames: Vec<HybridLatch<BufferFrame>>,
    free_frames: ArrayQueue<&'static HybridLatch<BufferFrame>>,
    free_pids: ArrayQueue<Pid>,
    cool_frames: ArrayQueue<&'static HybridLatch<BufferFrame>>,
    write_buffer_count: AtomicUsize
}

impl SizeClass {
    pub fn class_size(&self) -> usize {
        2usize.pow(self.class as u32)
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
        if let Err(e) = self.free_pids.push(pid) {
            unreachable!("should have space");
        }
    }

    pub fn allocate_page(&'static self, dtid: DataStructureId) -> Result<ExclusiveGuard<'static, BufferFrame>, BufMgrError> {
        let mut tries = 0;
        let free_bf = loop {
            if let Some(bf) = self.free_frames.pop() {
                break bf;
            }

            if tries > 10 { // TODO tune this value
                // println!("ALLOCATE");
                return Err(BufMgrError::OutOfFrames(self.class))
            }

            tries += 1;
        }; //.ok_or_else(|| BufMgrError::OutOfFrames(self.class))?;

        let free_pid = self.next_pid();

        // TODO assertNotExclusivelyLatched
        let mut bf = free_bf.exclusive();
        assert!(bf.state == BfState::Free);
        bf.pid = free_pid;
        bf.state = BfState::Hot;
        // bf.last_written_gsn = 0;
        bf.page.size_class = self.class as u64;
        bf.page.gsn = 0;
        bf.page.dtid = dtid;

        // if free_pid.page_id() > pool_size { println!("going larger than memory") }

        Ok(bf)
    }

    pub fn reclaim_page(&self, mut frame: ExclusiveGuard<'static, BufferFrame>) {
        assert_eq!(frame.page.size_class, self.class as u64);
        self.free_page(frame.pid);
        frame.reset();
        // should we unlock after the push?
        let unlocked = frame.unlock();
        if let Err(e) = self.free_frames.push(unlocked.latch()) {
            unreachable!("should have space");
        }
    }

    pub fn random_frame(&'static self) -> &'static HybridLatch<BufferFrame> {
        use rand::seq::SliceRandom;
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
            (Evicted, Load) => {
                let size_class = self.bufmgr.size_class(self.pid.size_class());
                let mut tries = 0;
                let free_bf = loop {
                    if let Some(bf) = size_class.free_frames.pop() {
                        break bf;
                    }

                    if tries > 100 { // TODO tune this value
                        return Err(BufMgrError::OutOfFrames(self.pid.size_class().into()));
                    }

                    tries += 1;
                }; //.ok_or_else(|| BufMgrError::OutOfFrames(self.class))?;

                let mut frame = free_bf.exclusive();

                assert!(frame.state == BfState::Free);

                self.bufmgr.read_page_sync(self.pid, frame.page)?;
                // frame.last_written_gsn = frame.page.gsn;
                frame.state = BfState::Loaded;
                frame.pid = self.pid;

                self.state = Loaded(free_bf);
                Ok(IoOutcome::Ok)
            }
            (Loaded(loaded_frame), Swizzle(mut swip_x_guard)) => {
                let mut frame = loaded_frame.exclusive();
                assert!(!swip_x_guard.is_ref());
                if let RefOrPid::Pid(p) = swip_x_guard.downcast() {
                    assert_eq!(self.pid, p);
                } else {
                    panic!("not pid");
                }
                swip_x_guard.to_ref(loaded_frame);
                frame.state = BfState::Hot; // VERIFY: set to hot after swizzled in?

                let outcome = IoOutcome::SwipAndFrame(swip_x_guard, loaded_frame);
                self.state = Swizzled(loaded_frame);
                Ok(outcome)
            }
            // TODO more transitions
            _ => {
                unreachable!("bug");
                Ok(IoOutcome::Noop)
            }
        }
    }
}

// TODO think about datastructure registry and page provider thread

pub struct BufferManager {
    fd: std::fs::File,
    pool_size: usize,
    offset: Arc<AtomicUsize>,
    classes: Vec<SizeClass>,
    // io_map: scc::HashMap<Pid, Arc<Mutex<IoSlot>>>,
    io_map: Mutex<HashMap<Pid, Arc<Mutex<IoSlot>>>>,
    registry: Registry,
    running: AtomicBool
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
const BASE_SIZE_CLASS: usize = 16; // 2 ^ 16 == 64k

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
                            page: page_ref
                        });

                        frames.push(latched_frame);
                    }

                    classes.push(SizeClass {
                        class,
                        offset: Arc::clone(&offset),
                        free_frames: ArrayQueue::new(frames.len()),
                        free_pids: ArrayQueue::new(frames.len()),
                        cool_frames: ArrayQueue::new(frames.len()),
                        write_buffer_count: AtomicUsize::new(0),
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
            // io_map: scc::HashMap::default(),
            io_map: Mutex::new(HashMap::default()),
            registry: Registry::new(),
            running: AtomicBool::new(false)
        }
    }

    pub fn init(&'static self) {
        for size_class in self.classes.iter() {
            for frame in size_class.frames.iter() {
                if let Err(e) = size_class.free_frames.push(frame) {
                    unreachable!("should have space");
                }
            }
        }
        self.running.store(true, Ordering::Release);
        let _ = std::thread::spawn(move || { // TODO keep handle
            self.page_provider()
        });
    }

    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn read_page_sync(&self, pid: Pid, page: &mut Page) -> Result<(), BufMgrError>{
        use nix::sys::uio::pread;
        use std::os::unix::io::AsRawFd;

        let size_class = self.size_class(pid.size_class());
        let size = size_class.class_size();
        let offset = pid.page_id();
        let mut slice = unsafe { std::slice::from_raw_parts_mut(page as *mut _ as *mut u8, size) };
        // println!("pid: {:?}, slice_len: {}, offset: {}", pid, slice.len(), offset);
        let n = pread(self.fd.as_raw_fd(), &mut slice, offset as i64).map_err(|e| BufMgrError::Io(e))?;
        assert!(n == size);
        Ok(())
    }

    pub fn write_page_sync(&self, pid: Pid, page: &Page) -> Result<(), BufMgrError>{
        use nix::sys::uio::pwrite;
        use std::os::unix::io::AsRawFd;

        let size_class = self.size_class(pid.size_class());
        let size = size_class.class_size();
        let offset = pid.page_id();
        let mut slice = unsafe { std::slice::from_raw_parts(page as *const _ as *const u8, size) };
        // println!("write pid: {:?}, slice_len: {}, offset: {}", pid, slice.len(), offset);
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

    // TODO use actual node capacity
    pub fn allocate_page_for<T>(&'static self, dtid: DataStructureId, size: usize) -> Result<(ExclusiveGuard<'static, BufferFrame>, usize), BufMgrError> {
        let overhead = std::mem::size_of::<Page>() + std::mem::size_of::<T>();
        let class = BASE_SIZE_CLASS.max(((size + overhead).next_power_of_two() as f64).log2().floor() as usize);
        let size_class = self.size_class(class);
        Ok((size_class.allocate_page(dtid)?, size_class.class_size() - overhead))
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
}

pub type OptSwipGuard<'a> = OptimisticGuard<'a, Swip<HybridLatch<BufferFrame>>, BufferFrame>;
pub type ShrSwipGuard<'a> = SharedGuard<'a, Swip<HybridLatch<BufferFrame>>, BufferFrame>;
pub type ExvSwipGuard<'a> = ExclusiveGuard<'a, Swip<HybridLatch<BufferFrame>>, BufferFrame>;


impl BufferManager {
    #[inline]
    pub fn resolve_swip_fast(
        &'static self,
        swip_guard: OptSwipGuard<'static>
    ) -> error::Result<(
        OptSwipGuard<'static>,
        &'static HybridLatch<BufferFrame>
    )> {
        match swip_guard.downcast() {
            RefOrPid::Ref(r) => {
                swip_guard.recheck()?;
                Ok((swip_guard, r))
            },
            RefOrPid::Cool(_) | RefOrPid::Pid(_) => {
                self.resolve_swip(swip_guard)
            }
        }
    }

    pub fn resolve_swip(
        &'static self,
        mut swip_guard: OptSwipGuard<'static>
    ) -> error::Result<(
        OptSwipGuard<'static>,
        &'static HybridLatch<BufferFrame>
    )> {
        match swip_guard.downcast() {
            RefOrPid::Ref(r) => {
                swip_guard.recheck()?;
                return Ok((swip_guard, r));
            },
            RefOrPid::Cool(r) => {
                swip_guard.recheck()?;
                let frame_guard = r.optimistic_or_spin();
                let mut swip_x_guard = swip_guard.to_exclusive()?;
                let mut frame_x_guard = frame_guard.to_exclusive()?;
                frame_x_guard.state = BfState::Hot;
                swip_x_guard.to_ref(r);

                return Ok((swip_x_guard.unlock(), r));
            },
            RefOrPid::Pid(pid) => {
                // TODO check if page is cooling

                swip_guard.recheck()?;

                // Got locked slot

                return self.with_slot_or_create(pid, IoSlot::new_evicted(self, pid), |slot_guard, returned| {
                    match &slot_guard.state {
                        IoState::Evicted => {
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
                                return (true, Ok((swip_guard, frame_ref)));
                            } else {
                                unreachable!("no other outcome expected");
                            }
                        }
                        IoState::Loaded(loaded_frame) => {
                            let swip_x_guard = match swip_guard.to_exclusive() {
                                Ok(guard) => guard,
                                Err(_) => {
                                    return (false, Err(error::Error::Unwind)); // Retry without cleanup, this can leak a loaded frame // TODO maybe perform cleanup?
                                }
                            };

                            if let IoOutcome::SwipAndFrame(swip_x_guard, frame_ref) = slot_guard.transition(IoCommand::Swizzle(swip_x_guard)).expect("failed to swizzle") {
                                let swip_guard = swip_x_guard.unlock();
                                return (true, Ok((swip_guard, frame_ref)));
                            } else {
                                unreachable!("no other outcome expected");
                            }
                        }
                        IoState::Swizzled(swizzled_frame) => {
                            return (true, Err(error::Error::Unwind));
                        }
                        IoState::Aborted  => {
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
            if Arc::strong_count(&slot) <= 2 {
                // Try to remove
                let mut map = self.io_map.lock();
                let slot_ref = map.get(&guard.pid).expect("must exist");
                if Arc::strong_count(slot_ref) <= 2 {
                    map.remove(&guard.pid);
                }
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

    pub fn page_provider(&'static self) {
        // WriteBuffer::new(10000);
        use rand::Rng;
        while self.running.load(Ordering::Acquire) {
            // break; // FIXME remove this to run this code
            for size_class in self.classes.iter() {
                let free_lower_bound = (size_class.frames.len() as f64  * 0.10).ceil() as usize;
                let needs_eviction = || size_class.free_frames.len() < free_lower_bound /* && rand::thread_rng().gen_range(0..100) < 50 */;
                let cool_lower_bound = (size_class.frames.len() as f64 * 0.20).ceil() as usize;
                let needs_cooling = || size_class.free_frames.len() + size_class.cool_frames.len() < cool_lower_bound;

                let mut frame = size_class.random_frame();

                // TODO Add cooling check

                // TODO Implement simple eviction of hot frames with pwrite

                let mut cooling_attempts = 0;

                loop {
                    let mut try_cool = || {
                        while needs_cooling() && cooling_attempts < 10 { // TODO tune
                            let mut guard = frame.optimistic_or_unwind()?;
                            let valid_candidate = guard.state == BfState::Hot && !frame.is_exclusively_latched();

                            if !valid_candidate {
                                frame = size_class.random_frame();
                                cooling_attempts += 1;
                                continue;
                            }

                            let dtid = guard.page.dtid;
                            guard.recheck()?;

                            let mut all_evicted = true;
                            let mut picked_child = false;
                            self.registry().get(dtid).expect("exists").iterate_children_swips(&guard, Box::new(|swip| {
                                match swip.try_downcast()? {
                                    RefOrPid::Pid(_) => {
                                        guard.recheck()?;
                                        Ok(true)
                                    },
                                    RefOrPid::Cool(_) => {
                                        all_evicted = false;
                                        guard.recheck()?;
                                        Ok(true)
                                    },
                                    RefOrPid::Ref(r) => {
                                        all_evicted = false;
                                        frame = r;
                                        guard.recheck()?;
                                        picked_child = true;
                                        Ok(false)
                                    }
                                }
                            }))?;

                            if picked_child {
                                continue;
                            }

                            if !all_evicted {
                                frame = size_class.random_frame();
                                cooling_attempts += 1;
                                continue;
                            }

                            guard.recheck()?;

                            let (result, guard) = self.registry().get(dtid).expect("exists").find_parent(guard)?;
                            match result {
                                ParentResult::Root => {
                                    // Cannot cool root, restart
                                    frame = size_class.random_frame();
                                    cooling_attempts += 1;
                                    continue;
                                }
                                ParentResult::Parent(swip_guard) => {
                                    // TODO check space utilization

                                    // TODO frame_x_guard can only be acquired and release while the cooling mutex is locked?

                                    let mut swip_x_guard = swip_guard.to_exclusive()?;
                                    let mut frame_x_guard = guard.to_exclusive()?;

                                    assert!(frame_x_guard.state == BfState::Hot);

                                    frame_x_guard.state = BfState::Cool;
                                    swip_x_guard.to_cool(frame_x_guard.latch());

                                    // TODO maybe drop guards before push?
                                    size_class.cool_frames.push(frame_x_guard.latch()).expect("has space");

                                    // println!("COOL");
                                }
                            }
                        }

                        cooling_attempts = 0;

                        error::Result::Ok(())
                    };

                    match try_cool() {
                        Ok(_) => break,
                        Err(_e) => {
                            frame = size_class.random_frame();
                            continue;
                        }
                    }
                }

                let evict_frame = |guard: OptimisticGuard<'static, BufferFrame>| {
                    let dtid = guard.page.dtid;
                    guard.recheck()?;

                    let (result, guard) = self.registry().get(dtid).expect("exists").find_parent(guard)?;
                    match result {
                        ParentResult::Root => {
                            unreachable!("cannot evict root");
                        }
                        ParentResult::Parent(swip_guard) => {
                            let mut swip_x_guard = swip_guard.to_exclusive()?;
                            let mut frame_x_guard = guard.to_exclusive()?;

                            // No need to erase from cooling, already poped

                            assert!(frame_x_guard.state == BfState::Cool);
                            swip_x_guard.to_pid(frame_x_guard.pid);

                            frame_x_guard.reset();

                            let unlocked = frame_x_guard.unlock();
                            if let Err(e) = size_class.free_frames.push(unlocked.latch()) {
                                unreachable!("should have space");
                            }
                        }
                    }

                    error::Result::Ok(())
                };

                if needs_eviction() {
                    let n_pages_to_evict = free_lower_bound - size_class.free_frames.len();

                    if n_pages_to_evict > 0 {
                        let mut n_pages_left_to_evict = n_pages_to_evict;

                        while n_pages_left_to_evict > 0 {
                            let cool_frame = match size_class.cool_frames.pop() {
                                Some(f) => f,
                                None => break
                            };

                            let mut try_flush = || {
                                let mut guard = cool_frame.optimistic_or_unwind()?;
                                if guard.state != BfState::Cool {
                                    return Ok(());
                                }

                                // TODO check io_map for pid?

                                n_pages_left_to_evict -= 1;

                                if true { // isDirty
                                    // Add to write buffer
                                } else {
                                    evict_frame(guard)?;
                                }

                                error::Result::Ok(())
                            };

                            match try_flush() {
                                Ok(_) => continue,
                                Err(_e) => {
                                    size_class.cool_frames.push(cool_frame).expect("has space"); // FIXME should we do this?
                                    continue;
                                } // FIXME check if we can leak a cool frame, maybe push it back
                            }
                        }
                    }
                }

                    // TODO TODO TODO continue pp thread full implementation

//                     let mut try_evict = || {
//                         while needs_eviction() {
//                             let mut guard = frame.optimistic_or_unwind()?;
//                             match guard.state {
//                                 BfState::Hot => {
//                                     let dtid = guard.page.dtid;
//                                     guard.recheck()?;
//                                     let mut all_evicted = true;
//                                     let mut picked_child = false;
//                                     self.registry().get(dtid).expect("exists").iterate_children_swips(&guard, Box::new(|swip| {
//                                         match swip.try_downcast()? {
//                                             RefOrPid::Pid(_) => {
//                                                 guard.recheck()?;
//                                                 Ok(true)
//                                             },
//                                             RefOrPid::Ref(r) => {
//                                                 all_evicted = false;
//                                                 frame = r;
//                                                 guard.recheck()?;
//                                                 picked_child = true;
//                                                 Ok(false)
//                                             }
//                                         }
//                                     }))?;
// 
//                                     if picked_child {
//                                         continue;
//                                     }
// 
//                                     if !all_evicted {
//                                         frame = size_class.random_frame();
//                                         continue;
//                                     }
// 
//                                     // println!("got candidate for eviction");
// 
//                                     let (result, guard) = self.registry().get(dtid).expect("exists").find_parent(guard)?;
//                                     match result {
//                                         ParentResult::Root => {
//                                             // Cannot evict root, restart
//                                             frame = size_class.random_frame();
//                                             continue;
//                                         }
//                                         ParentResult::Parent(swip_guard) => {
//                                             let mut swip_x_guard = swip_guard.to_exclusive()?;
//                                             let mut frame_x_guard = guard.to_exclusive()?;
//                                             let pid = frame_x_guard.pid;
// 
//                                             // FIXME FIXME FIXME the bug is here when we encounter one
//                                             // of those two bad states, this causes old data to be
//                                             // resolved somehow
//                                             //
//                                             // FIXED FIXED FIXED
//                                             //
//                                             // TODO TODO TODO Improve IoSlots and IoMap, current
//                                             // implementation works but is ugly, cleanup debug mess
// 
// 
//                                             match self.try_with_slot_or_create(pid, IoSlot::new_swizzled(self, pid, frame_x_guard.latch()), |slot_guard, returned| {
//                                                 match &slot_guard.state {
//                                                     IoState::Evicted => {
//                                                         unreachable!("cannot be evicted already");
//                                                     }
//                                                     IoState::Loaded(loaded_frame) => {
//                                                         unreachable!("cannot be loaded already");
//                                                     }
//                                                     IoState::Swizzled(_swizzled_frame) => {
//                                                         swip_x_guard.to_pid(pid);
//                                                         let _ = swip_x_guard.unlock();
// 
//                                                         self.write_page_sync(pid, frame_x_guard.page).expect("failed to write page");
// 
//                                                         frame_x_guard.reset();
// 
//                                                         let unlocked = frame_x_guard.unlock();
//                                                         if let Err(e) = size_class.free_frames.push(unlocked.latch()) {
//                                                             unreachable!("should have space");
//                                                         }
// 
//                                                         slot_guard.state = IoState::Evicted;
//                                                         // println!("EVICTED! {:?}", pid);
//                                                         return (true, Ok(()))
//                                                     }
//                                                     IoState::Aborted => {
//                                                         return (true, Err(error::Error::Unwind))
//                                                     }
//                                                 }
//                                             }) {
//                                                 Some(res) => res?,
//                                                 None => {
//                                                     // Could not lock slot
//                                                     println!("locked?");
//                                                     frame = size_class.random_frame();
//                                                     continue;
//                                                 }
//                                             };
//                                         }
//                                     }
//                                 }
//                                 BfState::Loaded => {
//                                     let pid = guard.pid;
//                                     guard.recheck()?;
// 
//                                     match self.try_with_slot(pid, |slot_guard| {
//                                         let mut frame_x_guard = match guard.to_exclusive() {
//                                             Ok(guard) => guard,
//                                             Err(err) => {
//                                                 return (false, Err(err));
//                                             }
//                                         };
// 
//                                         assert_eq!(BfState::Loaded, frame_x_guard.state);
// 
//                                         frame_x_guard.reset();
// 
//                                         let unlocked = frame_x_guard.unlock();
//                                         if let Err(e) = size_class.free_frames.push(unlocked.latch()) {
//                                             unreachable!("should have space");
//                                         }
//                                         slot_guard.state = IoState::Evicted;
//                                         // println!("UNLOADED! {:?}", pid);
//                                         return (true, Ok(()))
//                                     }) {
//                                         None => {
//                                             // retry, this slot is locked or not found
//                                             frame = size_class.random_frame();
//                                             continue;
//                                         }
//                                         _ => {}
//                                     }
//                                 }
//                                 _ => {
//                                     return Err(error::Error::Unwind);
//                                 }
//                             }
//                         }
// 
//                         error::Result::Ok(())
//                     };
// 
//                     match try_evict() {
//                         Ok(_) => break,
//                         Err(_e) => {
//                             frame = size_class.random_frame();
//                             continue;
//                         }
//                     }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {

//     let pool_size = 1 * 1024 * 1024;
//
//     let mut size_classes = vec!();
//
//     for i in 0usize..N_SIZE_CLASSES {
//         if let Some(class_size) = 2usize.checked_pow((BASE_SIZE_CLASS + i) as u32) {
//             let slots = if class_size <= pool_size {
//                 pool_size / class_size
//             } else {
//                 0
//             };
//
//             println!("2 ^ {}, amount: {:?}", BASE_SIZE_CLASS + i, slots);
//
//             if slots > 0 {
//                 let mut size_class = vec!();
//                 let addr = unsafe {
//                     nix::sys::mman::mmap(
//                         std::ptr::null_mut(),
//                         pool_size,
//                         ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
//                         MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS,
//                         -1,
//                         0
//                     )?
//                 };
//
//                 unsafe {
//                     nix::sys::mman::madvise(
//                         addr,
//                         pool_size,
//                         MmapAdvise::MADV_DONTFORK
//                     )?
//                 };
//
//                 assert!((addr as usize) % 4096 == 0);
//
//                 for slot_n in 0..slots {
//                     let ptr = unsafe { (addr as *mut u8).add(slot_n * class_size) } as *mut Page<Node>;
//                     println!("offset {}", ptr as usize);
//
//                     let page_ref: &'static mut Page<Node> = unsafe { &mut *ptr };
//
//                     let latched_frame = HybridLatch::new(BufferFrame {
//                         state: 0,
//                         pid: 0,
//                         page: page_ref
//                     });
//
//                     size_class.push(latched_frame);
//                 }
//                 size_classes.push(size_class);
//             }
//         } else {
//             println!("size class too large");
//         }
//     }


    println!("Hello, world!");

    let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("/tmp/state.db")?;
    let bufmgr: &'static BufferManager = Box::leak(Box::new(BufferManager::new(file, 1 * 1024 * 1024)));
    bufmgr.init();

//     {
//         let mut bf_guard = bufmgr.allocate_page_for(66000)?;
//
//         println!("bf.page.gsn {:?}", std::ptr::addr_of!(bf_guard.page.size_class) as usize);
//
//         let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| &mut bf.page.value);
//
//         let node = node_guard.as_mut();
//
//         node.capacity = 5;
//         node.len = 3;
//         node.data_mut()[0] = 3;
//
//         println!("Mapped {:?}, node.data = {:?}", node, std::ptr::addr_of!(node.data) as usize);
//
//         let bf_guard = ExclusiveGuard::unmap(node_guard);
//         bufmgr.reclaim_page(bf_guard);
//     }
//
//     {
//         let mut bf_guard = bufmgr.allocate_page_for(1)?;
//
//         println!("bf.page.gsn {:?}", std::ptr::addr_of!(bf_guard.page.size_class) as usize);
//
//         let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| &mut bf.page.value);
//
//         let node = node_guard.as_mut();
//
//         node.capacity = 5;
//         node.len = 3;
//         node.data_mut()[0] = 3;
//
//         println!("Mapped {:?}, node.data = {:?}", node, std::ptr::addr_of!(node.data) as usize);
//
//         let bf_guard = ExclusiveGuard::unmap(node_guard);
//         bufmgr.reclaim_page(bf_guard);
//     }

    // FIXME managed bfs
//     let (first_pid, first_ref) = {
//         let mut bf_guard = bufmgr.allocate_page_for(1)?;
//         let pid = bf_guard.pid;
//
//         let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| &mut bf.page.value);
//
//         let node = node_guard.as_mut();
//         node.capacity = 1;
//         node.len = 1;
//         node.swip = Swip::from_pid(Pid::default());
//         node.data_mut()[0] = 1;
//
//         (pid, node_guard.latch())
//     };
//
//     let (second_pid, second_ref) = {
//         let mut bf_guard = bufmgr.allocate_page_for(1)?;
//         let pid = bf_guard.pid;
//
//         let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| &mut bf.page.value);
//
//         let node = node_guard.as_mut();
//         node.capacity = 2;
//         node.len = 2;
//         node.swip = Swip::from_pid(first_pid);
//         node.data_mut()[0] = 2;
//
//         (pid, node_guard.latch())
//     };


    // FIXME Test resolve swip on managed bfs written to disk
//     {
//         bufmgr.offset.store(131072, Ordering::Relaxed);
//         let mut bf_guard = bufmgr.allocate_page_for(1)?;
//         let pid = bf_guard.pid;
//
//         let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| &mut bf.page.value);
//         let node = node_guard.as_mut();
//         node.capacity = 3;
//         node.len = 3;
//         node.swip = Swip::from_pid(Pid::new(65536, 16));
//         node.data_mut()[0] = 3;
//
//         let swip_guard: OptSwipGuard<Node> = OptimisticGuard::map(node_guard.unlock(), |node| &node.swip);
//         let (swip_guard, second_latch) = bufmgr.resolve_swip_fast(swip_guard)?;
//         let second_opt = second_latch.optimistic_or_spin();
//
//         let second_swip: OptSwipGuard<Node> = OptimisticGuard::map(second_opt, |bf| &bf.page.value.swip);
//         let (second_swip, first_latch) = bufmgr.resolve_swip_fast(second_swip)?;
//         let first_opt = first_latch.optimistic_or_spin();
//     }

    println!("{:?}", bufmgr.classes);

    // FIXME write
//     bufmgr.write_all_buffer_frames();

//     let size = 4096;
//     let addr = unsafe {
//         nix::sys::mman::mmap(
//             std::ptr::null_mut(),
//             size,
//             ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
//             MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS,
//             -1,
//             0
//         )?
//     };
//
//     println!("{}", std::mem::align_of::<HybridLatch<BufferFrame<FixedNode<256>>>>());
//
//     println!("{}", std::mem::align_of::<HybridLatch<FixedNode<256>>>());
//
//     let buffer: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(addr as *mut u8, size) };

    // let latch = unsafe { &mut *(addr as *mut HybridLatch<Node<256>>) };
//     let latch = unsafe { &mut *(addr as *mut HybridLatch<BufferFrame<Node>>) };
//
//     println!("{}", std::mem::align_of::<HybridLatch<BufferFrame<Node>>>());
//     println!("{}", std::mem::size_of::<HybridLatch<BufferFrame<Node>>>());
//
//     println!("latch {:?}", addr as usize);
//
//     let mut bf_guard = latch.exclusive();
//     println!("bf.page {:?}", std::ptr::addr_of!(bf_guard.page) as usize);
//
//     let mut node_guard: ExvNodeGuard = ExclusiveGuard::map(bf_guard, |bf| &mut bf.page.value);
//
//     let node = node_guard.as_mut();
//
//     node.capacity = 5;
//     node.len = 3;
//     node.data()[0] = 3;
//
//
//     println!("{:?}", &buffer[..512 + 50]);
//
//     // let (verified, rest) = LayoutVerified::<_, Node>::new_from_prefix(buffer).unwrap();
//
//     // let node = verified.into_ref();
//
//     println!("Mapped {:?}, addr = {:?}, node.data = {:?}", node, addr as usize, std::ptr::addr_of!(node.data) as usize);
    Ok(())
}
