use crossbeam_queue::ArrayQueue;
use io_uring::{opcode, types, IoUring};
use std::{os::unix::io::{AsRawFd, RawFd}, convert::TryInto};

use crate::latch::{SharedGuard, HybridLatch};
use super::{BufferFrame, swip::Pid};

#[derive(PartialEq, Eq, Debug)]
enum State {
    Free,
    Ready,
    Pending,
    Done
}

#[derive(Debug, Clone, Copy)]
struct PageMeta {
    pid: Pid,
    gsn: u64,
    frame: &'static HybridLatch<BufferFrame>,
}

enum Storage {
    Buffer(BufferStorage),
    Guard(GuardStorage),
}

struct BufferStorage {
    alignment_offset: usize,
    storage: Box<[u8]>,
    size: usize,
}

struct GuardStorage {
    guard: Option<SharedGuard<'static, BufferFrame>>,
}

pub(crate) enum LatchOrGuard {
    Latch(&'static HybridLatch<BufferFrame>),
    Guard(SharedGuard<'static, BufferFrame>),
}

struct Slot {
    state: State,
    storage: Storage,
    meta: Option<PageMeta>,
}

impl Slot {
    fn slot_bytes(&self) -> &[u8] {
        match self.storage {
            Storage::Buffer(ref stg) => {
                let storage = stg.storage.as_ref();
                &storage[stg.alignment_offset..(stg.alignment_offset + stg.size)]
            }
            Storage::Guard(ref stg) => {
                stg.guard.as_ref().expect("exists").page_bytes()
            }
        }
    }

    fn to_ready(&mut self, guard: SharedGuard<'static, BufferFrame>) {
        match self.state {
            State::Free => {
                let meta = PageMeta {
                    pid: guard.pid,
                    gsn: guard.page.gsn,
                    frame: guard.latch(),
                };

                self.meta = Some(meta);

                match self.storage {
                    Storage::Buffer(ref mut stg) => {
                        assert_eq!(stg.size, guard.page_bytes().len());
                        let storage = stg.storage.as_mut();
                        storage[stg.alignment_offset..(stg.alignment_offset + stg.size)].copy_from_slice(guard.page_bytes());
                    }
                    Storage::Guard(ref mut stg) => {
                        stg.guard = Some(guard);
                    }
                }

                self.state = State::Ready;
            }
            _ => panic!("only free slots can become ready")
        }
    }

    fn to_pending(&mut self) {
        match self.state {
            State::Ready => {
                // TODO return buffer for submission
                self.state = State::Pending;
            }
            _ => panic!("only ready slots can become pending")
        }
    }

    fn to_done(&mut self) {
        match self.state {
            State::Pending => {
                // TODO grab buffer back for reuse
                self.state = State::Done;
            }
            _ => panic!("only pending slots can become done")
        }
    }

    fn to_free(&mut self) {
        match self.state {
            State::Done => {
                self.meta.take();
                match self.storage {
                    Storage::Guard(ref mut stg) => {
                        stg.guard.take();
                    }
                    _ => {}
                }
                self.state = State::Free;
            }
            _ => panic!("only done slots can become free") // FIXME ?
        }
    }
}

//  struct Slot {
//      alignment_offset: usize,
//      storage: Box<[u8]>,
//      size: usize,
//      frame: Option<&'static HybridLatch<BufferFrame>>,
//  }

// TODO TODO TODO initialize or receive a reference to a ring and submit slots, (using slot index as
// user data?) and wait for completion events mapping back to the frame reference
//
// FIXME maybe the buffer must wrap around, check reference impl

fn aligned_boxed_slice(size: usize, alignment: usize) -> (usize, Box<[u8]>) {
    let storage_size = size + alignment;
    let storage = vec![0u8; storage_size].into_boxed_slice();
    let alignment_offset = alignment - (storage.as_ptr() as usize % alignment);
    (alignment_offset, storage)
}

pub(crate) struct WriteBuffer {
    n_slots: usize,
    slot_size: usize,
    free_slots: ArrayQueue<usize>, // TODO change this to some simple queue, this does not need to
                                   // be thread-safe
    slots: Vec<Slot>,
    ring: io_uring::IoUring,
    fd: RawFd,
}

impl WriteBuffer {
    pub(crate) fn new(fd: &impl AsRawFd, n_slots: usize, slot_size: usize) -> WriteBuffer {
        let free_slots = ArrayQueue::new(n_slots);
        let mut slots = vec![];

        let use_guard = n_slots * slot_size > 20 * 1024 * 1024;

        for i in 0..n_slots {
            free_slots.push(i).unwrap();

            let storage = if use_guard {
                Storage::Guard(GuardStorage { guard: None })
            } else {
                let (alignment_offset, storage) = aligned_boxed_slice(slot_size, 512);
                Storage::Buffer(BufferStorage { alignment_offset, storage, size: slot_size })
            };

            slots.push(Slot {
                storage,
                meta: None,
                state: State::Free
            });
        }
        // TODO Proposed Plan
        // - use size in bytes for buffer
        // - allocate buffer with mmap to ensure alignment, or maybe Box<[u8]> and align manually
        // - have a has_space_for fn
        // - on add copy data to current offset and store offset, size and bf guard on metadata
        // - submit events and match results to metadata
        WriteBuffer {
            n_slots,
            slot_size,
            free_slots,
            slots,
            ring: IoUring::new(n_slots as u32).unwrap(),
            fd: fd.as_raw_fd(),
        }
    }

//      pub(crate) fn has_space_for(&self, size: usize) -> bool {
//          (self.offset + size) <= self.size
//      }

    pub(crate) fn is_full(&self) -> bool {
        self.free_slots.is_empty()
    }

    pub(crate) fn add(&mut self, guard: SharedGuard<'static, BufferFrame>) {
        assert!(!self.is_full());

        let slot_idx = self.free_slots.pop().unwrap();

        let slot = &mut self.slots[slot_idx];

        slot.to_ready(guard);

        let entry = io_uring::opcode::Write::new(
            io_uring::types::Fd(self.fd),
            slot.slot_bytes().as_ptr(),
            slot.slot_bytes().len().try_into().expect("too large")
        )
            .offset(slot.meta.expect("exists").pid.page_id().try_into().expect("too large"))
            .build()
            .user_data(slot_idx as u64);

        unsafe { self.ring.submission().push(&entry).expect("must not be full") };
    }

    pub(crate) fn submit(&mut self) {
        let ready_slots: Vec<_> = self.slots.iter_mut().filter(|s| s.state == State::Ready).collect();
        if ready_slots.len() > 0 {
            self.ring.submit().expect("failed to submit");
            for slot in ready_slots {
                slot.to_pending();
            }
        }
    }

    pub(crate) fn poll_events_sync(&mut self) -> usize {
        let mut count = 0;
        for entry in self.ring.completion() {
            let result = entry.result();
            let slot_idx = entry.user_data() as usize;

            let slot = &mut self.slots[slot_idx];
            // println!("result = {}", result);
            assert_eq!(slot.slot_bytes().len(), result as usize);
            assert_eq!(State::Pending, slot.state);

            slot.to_done();
            count += 1;
        }

        self.ring.completion().sync();

        count
    }

    pub(crate) fn done_items(&mut self) -> Vec<(LatchOrGuard, u64)> {
        let free_slots = &self.free_slots;
        self.slots.iter_mut()
            .enumerate()
            .filter(|(i, s)| s.state == State::Done)
            .map(|(i, s)| {
                let meta = s.meta.take().expect("exists");
                let written_gsn = meta.gsn;
                let latch_or_guard = match s.storage {
                    Storage::Guard(ref mut stg) => {
                        LatchOrGuard::Guard(stg.guard.take().expect("must exist"))
                    }
                    Storage::Buffer(_) => {
                        LatchOrGuard::Latch(meta.frame)
                    }
                };
                s.to_free();
                free_slots.push(i).unwrap();
                (latch_or_guard, written_gsn)
            })
            .collect()
    }
}


#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::latch::HybridLatch;
    use nix::sys::mman::{ProtFlags, MapFlags, MmapAdvise};

    use crate::persistent::bufmgr::{BufferFrame, Page, BfState, swip::Pid};

    use super::WriteBuffer;

    fn test_frames(n_pages: usize) -> &'static [HybridLatch<BufferFrame>] {
        let mut frames = vec![];
        let class = 16;
        let class_size = 2usize.pow(class);
        let pool_size = class_size * n_pages;

        let addr = unsafe {
            nix::sys::mman::mmap(
                std::ptr::null_mut(),
                pool_size,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS,
                -1,
                0
                ).expect("failed to init test frames")
        };
        unsafe {
            nix::sys::mman::madvise(
                addr,
                pool_size,
                MmapAdvise::MADV_DONTFORK
                ).expect("failed to configure pool")
        };

        for frame_idx in 0..n_pages {
            let ptr = unsafe { (addr as *mut u8).add(frame_idx * class_size) } as *mut Page;

            let page_ref: &'static mut Page = unsafe { &mut *ptr };

            page_ref.size_class = class as u64;

            let page_id = frame_idx * class_size;

            let latched_frame = HybridLatch::new(BufferFrame {
                state: BfState::Free,
                pid: Pid::new(page_id as u64, class as u8),
                last_written_gsn: 0,
                writting: false,
                page: page_ref
            });

            frames.push(latched_frame);
        }

        frames.leak()
    }

    #[test]
    fn write_buffer_simple_write() {
        let file = File::create("/tmp/test.bin").expect("can create");
        let mut buffer = WriteBuffer::new(&file, 3, 2usize.pow(16));

        let frames = test_frames(1);

        let frame = frames.first().unwrap();

        assert!(!buffer.is_full());

        {
            let guard = frame.shared();
            buffer.add(guard);
        }

        buffer.submit();

        let n = buffer.poll_events_sync();

        assert!(n == 0);

        std::thread::sleep(std::time::Duration::from_millis(100));

        let n = buffer.poll_events_sync();

        assert!(n > 0);

        let items = buffer.done_items();

        assert!(items.len() == 1);
    }

    #[test]
    fn write_buffer_full_flush() {
        let file = File::create("/tmp/test.bin").expect("can create");
        let mut buffer = WriteBuffer::new(&file, 4, 2usize.pow(16));

        let frames = test_frames(4);

        assert!(!buffer.is_full());

        for frame in frames.iter() {
            let guard = frame.shared();
            buffer.add(guard);
        }

        buffer.submit();

        let n = buffer.poll_events_sync();

        assert!(n == 0);

        std::thread::sleep(std::time::Duration::from_millis(100));

        let n = buffer.poll_events_sync();

        assert!(n == 4);

        let items = buffer.done_items();

        assert!(items.len() == 4);
    }

    #[test]
    fn write_buffer_reuse() {
        let file = File::create("/tmp/test.bin").expect("can create");
        let mut buffer = WriteBuffer::new(&file, 4, 2usize.pow(16));

        let frames = test_frames(8);

        let mut remaining_frames: Vec<_> = frames.iter().collect();

        let mut complete_frames = vec![];

        while complete_frames.len() < frames.len() {
            while remaining_frames.len() > 0 && !buffer.is_full() {
                let frame = remaining_frames.pop().unwrap();
                let guard = frame.shared();
                buffer.add(guard);
            }

            buffer.submit();

            let n = buffer.poll_events_sync();

            let items = buffer.done_items();

            assert_eq!(n, items.len());

            complete_frames.extend(items);
        }

        assert_eq!(frames.len(), complete_frames.len());
    }
}
