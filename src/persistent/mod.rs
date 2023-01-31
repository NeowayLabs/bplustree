use nix::libc::O_DIRECT;
use once_cell::sync::OnceCell;

use std::fmt;
use std::os::unix::prelude::OpenOptionsExt;
use std::slice;

mod bufmgr;

mod node;

mod tree;

mod iter;

// mod recovery;

mod debug;

#[cfg(test)]
mod bench;

use bufmgr::{
    swip::{Swip, Pid},
    BufferManager,
    BufferFrame
};


use node::{
    Node
};

pub use tree::PersistentBPlusTree;

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
            // .custom_flags(O_DIRECT)
            .open(path)?;

    BUFMGR.set(BufferManager::new(file, pool_size)).expect("failed to set global");
    bufmgr().init();

    Ok(())
}

pub fn ensure_global_bufmgr<P: AsRef<std::path::Path>>(path: P, pool_size: usize) -> Result<(), std::io::Error> {
    let mut needs_init = false;
    let bufmgr = BUFMGR.get_or_try_init(|| {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // .custom_flags(O_DIRECT)
            .open(path)?;

        needs_init = true;
        std::io::Result::Ok(BufferManager::new(file, pool_size))
    })?;

    if needs_init {
        bufmgr.init();
    }

    Ok(())
}

pub fn new_leaked_bufmgr<P: AsRef<std::path::Path>>(path: P, pool_size: usize) -> Result<&'static BufferManager, std::io::Error> {
    let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // .custom_flags(O_DIRECT)
            .open(path)?;

    let bufmgr = BufferManager::new(file, pool_size);
    let bufmgr = Box::leak(Box::new(bufmgr));
    bufmgr.init();

    Ok(bufmgr)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_global_bufmgr("/tmp/state.db", 1 * 1024 * 1024)?;

    Ok(())
}
