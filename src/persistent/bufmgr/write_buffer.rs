use io_uring::{opcode, types, IoUring};
use std::os::unix::io::AsRawFd;

use bplustree::latch::{OptimisticGuard, HybridLatch};
use super::{BufferFrame};

struct Slot {
    offset: usize,
    len: usize,
    frame: &'static HybridLatch<BufferFrame>
}

pub(crate) struct WriteBuffer {
    size: usize,
    offset: usize,
    storage: Box<[u8]>,
    alignment_offset: usize,
    slots: Vec<Slot>
}

impl WriteBuffer {
    pub(crate) fn new(size: usize) -> WriteBuffer {
        let storage_size = size + 512;
        let storage = vec![0u8; storage_size].into_boxed_slice();
        let alignment_offset = 512 - (storage.as_ptr() as usize % 512);

//         let aligned = storage[alignment_offset..].as_ptr() as usize % 512;
//         println!("storage_size = {}, alignment_offset = {}, aligned = {}", storage_size, alignment_offset, aligned);

        // TODO Proposed Plan
        // - use size in bytes for buffer
        // - allocate buffer with mmap to ensure alignment, or maybe Box<[u8]> and align manually
        // - have a has_space_for fn
        // - on add copy data to current offset and store offset, size and bf guard on metadata
        // - submit events and match results to metadata
        WriteBuffer {
            size,
            offset: 0,
            storage,
            alignment_offset,
            slots: vec!()
        }
    }

    pub(crate) fn has_space_for(&self, size: usize) -> bool {
        (self.offset + size) <= self.size
    }

    pub(crate) fn add(&self, guard: SharedGuard<'static, BufferFrame>) {
        assert!(self.has_space_for(guard.page_bytes().len()));
    }
}
