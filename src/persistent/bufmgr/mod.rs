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
    atomic::{AtomicUsize, Ordering},
    Arc
};

pub mod swip;

use swip::{Pid, Swip, RefOrPid};

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Page<T> {
    size_class: u64,
    gsn: u64,
    // datastructure_id ?
    pub(crate) value: T
}

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
#[repr(C)]
enum BfState {
    Free,
    Hot,
    Cool,
    Loaded
}

#[derive(Debug)]
#[repr(C)]
pub struct BufferFrame<T: 'static> {
    state: BfState,
    pid: Pid,
    // padding: [u8; 512 - (std::mem::size_of::<HybridLatch<()>>() + (std::mem::size_of::<u64>() * 2))], // Alignment hack, very dependent on BufferFrame fields
    pub(crate) page: &'static mut Page<T>
}

impl<T> BufferFrame<T> {
    fn reset (&mut self) {
        // self.last_written_gsn = 0;
        self.state = BfState::Free;
        self.pid = Pid::default();
        // TODO clear page?
    }
}

unsafe impl<T> Sync for BufferFrame<T> {}
unsafe impl<T> Send for BufferFrame<T> {}

#[repr(C)]
pub struct Node {
    capacity: u64,
    len: u64,
    swip: Swip<HybridLatch<BufferFrame<Node>>>,
    data: () // Data starts at `addr_of(self.data)` and ends at `addr_of(self.data) + self.capacity`
}

impl Node {
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(std::ptr::addr_of!(self.data) as *const u8, self.capacity as usize) }
    }

    #[inline]
    fn data_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(std::ptr::addr_of_mut!(self.data) as *mut u8, self.capacity as usize) }
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("capacity", &self.capacity)
            .field("len", &self.len)
            .field("swip", &self.swip.as_raw())
            .field("data", &self.data())
            .finish()
    }
}

pub trait PageValue {
    type SwipTag;

    fn swip(&self, tag: Self::SwipTag) -> Option<&Swip<HybridLatch<BufferFrame<Self>>>> where Self: Sized;

    fn swip_mut(&mut self, tag: Self::SwipTag) -> Option<&mut Swip<HybridLatch<BufferFrame<Self>>>> where Self: Sized;
}

#[derive(Debug)]
pub struct SizeClass<T: 'static> {
    class: usize,
    offset: Arc<AtomicUsize>,
    frames: Vec<HybridLatch<BufferFrame<T>>>,
    free_frames: ArrayQueue<&'static HybridLatch<BufferFrame<T>>>,
    free_pids: ArrayQueue<Pid>
}

impl <T: 'static> SizeClass<T> {
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

    // TODO implement reclaim page
    pub fn free_page(&self, pid: Pid) {
        if let Err(e) = self.free_pids.push(pid) {
            unreachable!("should have space");
        }
    }

    pub fn allocate_page(&'static self) -> Result<ExclusiveGuard<'static, BufferFrame<T>>, BufMgrError> {
        // TODO keep trying until it returns a free frame?
        let free_bf = self.free_frames.pop().ok_or_else(|| BufMgrError::OutOfFrames(self.class))?;
        let free_pid = self.next_pid();

        // TODO assertNotExclusivelyLatched
        let mut bf = free_bf.exclusive();
        assert!(bf.state == BfState::Free);
        bf.pid = free_pid;
        bf.state = BfState::Hot;
        // bf.last_written_gsn = 0;
        bf.page.size_class = self.class as u64;
        bf.page.gsn = 0;

        // if free_pid.page_id() > pool_size { println!("going larger than memory") }

        Ok(bf)
    }

    pub fn reclaim_page(&self, mut frame: ExclusiveGuard<'static, BufferFrame<T>>) {
        assert_eq!(frame.page.size_class, self.class as u64);
        self.free_page(frame.pid);
        frame.reset();
        // should we unlock after the push?
        let unlocked = frame.unlock();
        if let Err(e) = self.free_frames.push(unlocked.latch()) {
            unreachable!("should have space");
        }
    }
}

enum IoCommand<T: 'static> {
    Load,
    Swizzle(ExvSwipGuard<'static, T>),
    Evict(ExclusiveGuard<'static, BufferFrame<T>>)
}

enum IoOutcome<T: 'static> {
    Ok,
    SwipAndFrame(ExvSwipGuard<'static, T>, &'static HybridLatch<BufferFrame<T>>),
    Noop
}

#[derive(Debug)]
enum IoState<T: 'static> {
    Evicted,
    Loaded(&'static HybridLatch<BufferFrame<T>>),
    Swizzled(&'static HybridLatch<BufferFrame<T>>)
}

#[derive(Debug)]
struct IoSlot<T: 'static> {
    bufmgr: &'static BufferManager<T>,
    state: IoState<T>,
    pid: Pid
}

impl<T> IoSlot<T> {
    fn new_evicted(bufmgr: &'static BufferManager<T>, pid: Pid) -> Self {
        IoSlot {
            bufmgr,
            state: IoState::Evicted,
            pid
        }
    }

    fn transition(&mut self, command: IoCommand<T>) -> Result<IoOutcome<T>, BufMgrError> {
        use IoState::*;
        use IoCommand::*;

        match (&self.state, command)  {
            (Evicted, Load) => {
                let size_class = self.bufmgr.size_class(self.pid.size_class());
                let free_bf = size_class.free_frames.pop().ok_or_else(|| BufMgrError::OutOfFrames(self.pid.size_class().into()))?;
                // assert!(free_bf.state == BfState::Free);
                //
                let mut frame = free_bf.exclusive();
                self.bufmgr.read_page_sync(self.pid, frame.page)?;
                // frame.last_written_gsn = frame.page.gsn;
                frame.state = BfState::Loaded;
                frame.pid = self.pid;

                self.state = Loaded(free_bf);
                Ok(IoOutcome::Ok)
            }
            (Loaded(loaded_frame), Swizzle(mut swip_x_guard)) => {
                let mut frame = loaded_frame.exclusive();
                swip_x_guard.to_ref(loaded_frame);
                frame.state = BfState::Hot; // VERIFY: set to hot after swizzled in?

                let outcome = IoOutcome::SwipAndFrame(swip_x_guard, loaded_frame);
                self.state = Swizzled(loaded_frame);
                Ok(outcome)
            }
            // TODO more transitions
            _ => {
                Ok(IoOutcome::Noop)
            }
        }
    }
}

// TODO think about datastructure registry and page provider thread

pub struct BufferManager<T: 'static> {
    fd: std::fs::File,
    offset: Arc<AtomicUsize>,
    classes: Vec<SizeClass<T>>,
    io_map: scc::HashMap<Pid, Arc<Mutex<IoSlot<T>>>>
}

impl<T: fmt::Debug> fmt::Debug for BufferManager<T> {
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

impl<T: 'static> BufferManager<T> {
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
                        let ptr = unsafe { (addr as *mut u8).add(frame_idx * class_size) } as *mut Page<T>;

                        let page_ref: &'static mut Page<T> = unsafe { &mut *ptr };

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
                        frames
                    });
                }
            }
        }

        BufferManager {
            fd,
            offset,
            classes,
            io_map: scc::HashMap::default()
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
    }

    pub fn read_page_sync(&self, pid: Pid, page: &mut Page<T>) -> Result<(), BufMgrError>{
        use nix::sys::uio::pread;
        use std::os::unix::io::AsRawFd;

        let size_class = self.size_class(pid.size_class());
        let size = size_class.class_size();
        let offset = pid.page_id();
        let mut slice = unsafe { std::slice::from_raw_parts_mut(page as *mut _ as *mut u8, size) };
        let n = pread(self.fd.as_raw_fd(), &mut slice, offset as i64).map_err(|e| BufMgrError::Io(e))?;
        assert!(n == size);
        Ok(())
    }

    #[inline]
    pub fn size_class<U>(&self, size_class: U) -> &SizeClass<T>
    where
        U: std::convert::TryInto<u64>,
        U::Error: std::fmt::Debug
    {
        let size_class = size_class.try_into().expect("failed to convert size class");
        //println!("{}", size_class);
        // println!("{}", self.classes.len());
        &self.classes[size_class as usize - BASE_SIZE_CLASS]
    }

    // TODO use actual node capacity
    pub fn allocate_page_for(&'static self, size: usize) -> Result<(ExclusiveGuard<'static, BufferFrame<T>>, usize), BufMgrError> {
        let class = BASE_SIZE_CLASS.max(((size + std::mem::size_of::<Page<T>>()).next_power_of_two() as f64).log2().floor() as usize);
        let size_class = self.size_class(class);
        Ok((size_class.allocate_page()?, size_class.class_size() - std::mem::size_of::<Page<T>>()))
    }

    pub fn reclaim_page(&self, mut frame: ExclusiveGuard<'static, BufferFrame<T>>) {
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

pub type OptSwipGuard<'a, T> = OptimisticGuard<'a, Swip<HybridLatch<BufferFrame<T>>>, BufferFrame<T>>;
pub type ShrSwipGuard<'a, T> = SharedGuard<'a, Swip<HybridLatch<BufferFrame<T>>>, BufferFrame<T>>;
pub type ExvSwipGuard<'a, T> = ExclusiveGuard<'a, Swip<HybridLatch<BufferFrame<T>>>, BufferFrame<T>>;


impl<T: 'static> BufferManager<T> {
    #[inline]
    pub fn resolve_swip_fast(
        &'static self,
        swip_guard: OptSwipGuard<'static, T>
    ) -> error::Result<(
        OptSwipGuard<'static, T>,
        &'static HybridLatch<BufferFrame<T>>
    )> {
        match swip_guard.downcast() {
            RefOrPid::Ref(r) => {
                swip_guard.recheck()?;
                Ok((swip_guard, r))
            },
            RefOrPid::Pid(pid) => {
                self.resolve_swip(swip_guard)
            }
        }
    }

    pub fn resolve_swip(
        &'static self,
        mut swip_guard: OptSwipGuard<'static, T>
    ) -> error::Result<(
        OptSwipGuard<'static, T>,
        &'static HybridLatch<BufferFrame<T>>
    )> {
        match swip_guard.downcast() {
            RefOrPid::Ref(r) => {
                swip_guard.recheck()?;
                return Ok((swip_guard, r));
            },
            RefOrPid::Pid(pid) => {
                // TODO check if page is cooling

                swip_guard.recheck()?;

                let mut existed = false;
                let mut slot = Arc::new(Mutex::new(IoSlot::new_evicted(self, pid)));
                let create_slot = Arc::clone(&slot);
                let tmp_slot = Arc::clone(&slot);
                let mut slot_guard = tmp_slot.lock();
                self.io_map.upsert(
                    pid,
                    move || create_slot,
                    |_, v| {
                        slot = Arc::clone(v);
                        existed = true;
                    }
                );

                if existed {
                    drop(slot_guard);
                    slot_guard = slot.lock();
                }

                // Got locked slot

                match &slot_guard.state {
                    IoState::Evicted => {
                        let _ = slot_guard.transition(IoCommand::Load).expect("failed to load");
                        let swip_x_guard = swip_guard.to_exclusive()?;
                        if let IoOutcome::SwipAndFrame(swip_x_guard, frame_ref) = slot_guard.transition(IoCommand::Swizzle(swip_x_guard)).expect("failed to swizzle") {
                            let _ = self.io_map.remove(&pid);
                            let swip_guard = swip_x_guard.unlock();
                            return Ok((swip_guard, frame_ref));
                        } else {
                            unreachable!("no other outcome expected");
                        }
                    }
                    IoState::Loaded(loaded_frame) => {
                        let swip_x_guard = swip_guard.to_exclusive()?;
                        if let IoOutcome::SwipAndFrame(swip_x_guard, frame_ref) = slot_guard.transition(IoCommand::Swizzle(swip_x_guard)).expect("failed to swizzle") {
                            let _ = self.io_map.remove(&pid);
                            let swip_guard = swip_x_guard.unlock();
                            return Ok((swip_guard, frame_ref));
                        } else {
                            unreachable!("no other outcome expected");
                        }
                    }
                    IoState::Swizzled(swizzled_frame) => {
                        let _ = self.io_map.remove(&pid);
                        swip_guard.recheck()?;
                        return Ok((swip_guard, swizzled_frame));
                    }
                }
            }
        };
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
    let bufmgr: &'static BufferManager<Node> = Box::leak(Box::new(BufferManager::<Node>::new(file, 1 * 1024 * 1024)));
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
