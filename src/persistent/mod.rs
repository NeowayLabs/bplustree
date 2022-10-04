use once_cell::sync::OnceCell;

use std::fmt;
use std::slice;

mod bufmgr;

mod node;

mod tree;

mod iter;

// mod recovery;

#[cfg(test)]
mod bench;

use bufmgr::{
    swip::{Swip, Pid},
    BufferManager,
    BufferFrame
};

use bplustree::{
    error,
    latch::HybridLatch
};

use node::{
    Node
};

static BUFMGR: OnceCell<BufferManager> = OnceCell::new();

#[inline]
pub fn bufmgr() -> &'static BufferManager {
    unsafe { BUFMGR.get_unchecked() }
}

pub fn setup_global_bufmgr<P: AsRef<std::path::Path>>(path: P, pool_size: usize) -> Result<(), std::io::Error> {
    let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

    BUFMGR.set(BufferManager::new(file, pool_size)).expect("failed to set global");
    bufmgr().init();

    Ok(())
}

pub(crate) fn ensure_global_bufmgr<P: AsRef<std::path::Path>>(path: P, pool_size: usize) -> Result<(), std::io::Error> {
    let mut needs_init = false;
    let bufmgr = BUFMGR.get_or_try_init(|| {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        needs_init = true;
        std::io::Result::Ok(BufferManager::new(file, pool_size))
    })?;

    if needs_init {
        bufmgr.init();
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_global_bufmgr("/tmp/state.db", 1 * 1024 * 1024)?;

    Ok(())
}
