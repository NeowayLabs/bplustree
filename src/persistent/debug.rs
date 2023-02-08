use std::cell::RefCell;
use std::thread::ThreadId;
use smallvec::SmallVec;
use crossbeam_queue::SegQueue;

use crate::persistent::bufmgr::registry::FrameDebugInfo;

pub(crate) static OPS: SegQueue<Op> = SegQueue::new();

thread_local!(pub(crate) static LOCAL_OPS: RefCell<SmallVec<[LocalOp; 2048]>> = RefCell::new(SmallVec::new()));

pub(crate) fn to_u64(slice: &[u8]) -> u64 {
    use std::convert::TryInto;
    let array = if slice.len() == 9 || slice.len() == 128 * 1024 {
        slice[..8].try_into()
    } else {
        slice.try_into()
    };

    if array.is_err() {
        println!("BUG got slice with len: {}, content: {:?}", slice.len(), slice);
    }
    u64::from_be_bytes(array.unwrap())
}

#[derive(Debug)]
pub(crate) enum Op {
    Split {
        p_len: usize,
        p_pos: usize,
        split_key: u64,
        old_lower: u64,
        old_upper: u64,
        l_lower: u64,
        l_upper: u64,
        r_lower: u64,
        r_upper: u64,
        tid: ThreadId,
    },
    MergeLeft {
        p_len: usize,
        p_pos: usize,
        pos_key: u64,
        prev_key: u64,
        l_lower: u64,
        l_upper: u64,
        r_lower: u64,
        r_upper: u64,
        r_latch: u64,
        new_lower: u64,
        new_upper: u64,
        tid: ThreadId,
    },
    MergeRight {
        p_len: usize,
        p_pos: usize,
        pos_key: u64,
        next_key: u64,
        l_lower: u64,
        l_upper: u64,
        r_lower: u64,
        r_upper: u64,
        r_latch: u64,
        new_lower: u64,
        new_upper: u64,
        tid: ThreadId,
    },
    SwizzleIn {
        parent: FrameDebugInfo,
        loaded: FrameDebugInfo,
    },
    EvictOut {
        parent: FrameDebugInfo,
        loaded: FrameDebugInfo,
    }
}

#[macro_export]
macro_rules! dbg_swizzle_in {
    ($bufmgr:expr, $swip:expr, $frame:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            use crate::persistent::debug::{OPS, Op};
            let dt = $bufmgr.registry().get($frame.page.dtid).unwrap();
            OPS.push(Op::SwizzleIn {
                parent: dt.debug_info($swip.as_unmapped()),
                loaded: dt.debug_info($frame),
            });
        };
    };
}

#[macro_export]
macro_rules! dbg_evict_out {
    ($bufmgr:expr, $swip:expr, $frame:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            use crate::persistent::debug::{OPS, Op};
            let dt = $bufmgr.registry().get($frame.page.dtid).unwrap();
            let op = Op::EvictOut {
                parent: dt.debug_info($swip.as_unmapped()),
                loaded: dt.debug_info($frame),
            };
            // println!("{:?}", op);
            OPS.push(op);
        };
    };
}

#[macro_export]
macro_rules! dbg_split_prepare {
    ($val:ident, $old:expr) => {
        #[cfg(feature = "deepdebug")]
        let $val = {
            use super::node::NodeKind;
            use crate::persistent::debug::{to_u64, OPS, Op};
            let lower = to_u64($old.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
            let upper = to_u64($old.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
            (lower, upper)
        };
    };
}

#[macro_export]
macro_rules! dbg_split {
    ($val:ident, $left:expr, $right:expr, $len:expr, $pos:expr, $key:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            use super::node::NodeKind;
            use crate::persistent::debug::{to_u64, OPS, Op};
            let l_lower = to_u64($left.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
            let l_upper = to_u64($left.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));

            let r_lower = to_u64($right.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
            let r_upper = to_u64($right.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
            let old_lower = $val.0;
            let old_upper = $val.1;

            let split_key = to_u64($key);

            let p_len = $len;
            let p_pos = $pos;
            let tid = std::thread::current().id();

            OPS.push(Op::Split {
                p_len, p_pos, split_key, old_lower, old_upper, l_lower, l_upper, r_upper, r_lower, tid
            });
        }
    };
}

#[macro_export]
macro_rules! dbg_merge_prepare {
    ($val:ident, $left:expr, $right:expr, $r_latch:expr) => {
        #[cfg(feature = "deepdebug")]
        let $val = {
            use super::node::NodeKind;
            use crate::persistent::debug::{to_u64, OPS, Op};
            let l_lower = to_u64($left.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
            let l_upper = to_u64($left.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
            let r_lower = to_u64($right.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
            let r_upper = to_u64($right.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
            let r_latch = $r_latch;
            (l_lower, l_upper, r_lower, r_upper, r_latch)
        };
    };
}

#[macro_export]
macro_rules! dbg_merge_left {
    ($val:ident, $new:expr, $parent_guard_x:expr, $pos:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            use super::node::NodeKind;
            use crate::persistent::debug::{to_u64, OPS, Op};
            let parent_internal = $parent_guard_x.as_internal();
            let p_pos = $pos;
            let p_len = parent_internal.base.len();
            let pos_key = to_u64(parent_internal.full_key_at(p_pos).unopt().as_slice());
            let prev_key = to_u64(parent_internal.full_key_at(p_pos - 1).unopt().as_slice());
            let new_lower = to_u64($new.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
            let new_upper = to_u64($new.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
            let (l_lower, l_upper, r_lower, r_upper, r_latch) = $val;

            let tid = std::thread::current().id();

            let merge_left = Op::MergeLeft {
                p_len,
                p_pos,
                pos_key,
                prev_key,
                l_lower,
                l_upper,
                r_lower,
                r_upper,
                r_latch,
                new_lower,
                new_upper,
                tid,
            };

            OPS.push(merge_left);
        }
    };
}

#[macro_export]
macro_rules! dbg_merge_right {
    ($val:ident, $new:expr, $parent_guard_x:expr, $pos:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            use super::node::NodeKind;
            use crate::persistent::debug::{to_u64, OPS, Op};
            let parent_internal = $parent_guard_x.as_internal();
            let p_pos = $pos;
            let p_len = parent_internal.base.len();

            let pos_key = to_u64(parent_internal.full_key_at(p_pos).unopt().as_slice());
            let next_key = if p_pos + 1 == p_len {
                std::u64::MAX
            } else {
                to_u64(parent_internal.full_key_at(p_pos + 1).unopt().as_slice())
            };

            let new_lower = to_u64($new.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
            let new_upper = to_u64($new.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
            let (l_lower, l_upper, r_lower, r_upper, r_latch) = $val;

            let tid = std::thread::current().id();

            let merge_right = Op::MergeRight {
                p_len,
                p_pos,
                pos_key,
                next_key,
                l_lower,
                l_upper,
                r_lower,
                r_upper,
                r_latch,
                new_lower,
                new_upper,
                tid,
            };

            OPS.push(merge_right);
        }
    };
}

#[macro_export]
macro_rules! dbg_global_report {
    () => {
        #[cfg(feature = "deepdebug")]
        {
            use crate::persistent::debug::OPS;

            while let Some(op) = OPS.pop() {
                println!("{:?}", op);
            };
        }
    };
}

#[derive(Debug)]
pub(crate) enum LocalOp {
    FindParentStepInner { lower: u64, upper: u64, key: u64, pos: u64 },
    FindParentStepLeaf { lower: u64, upper: u64, key: u64 },
    FindParentPtrCmp { swip_ptr: u64, latch_ptr: u64 },
    Debug(u64),
}

#[macro_export]
macro_rules! dbg_tag {
    ($tag:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            use crate::persistent::debug::{LOCAL_OPS, LocalOp};
            LOCAL_OPS.with(|ops| {
                if ops.borrow().len() < 2048 {
                    ops.borrow_mut().push(LocalOp::Debug($tag));
                } else {
                    println!("full");
                }
            });
        }
    };
}


#[macro_export]
macro_rules! dbg_find_parent_ptr_cmp {
    ($swip_guard:expr, $needle:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            let try_store = || {
                use super::node::NodeKind;
                use crate::persistent::debug::{LOCAL_OPS, LocalOp};

                let swip_ptr = $swip_guard.as_hot_ptr();
                let latch_ptr = $needle.latch() as *const _ as u64;
                $swip_guard.recheck()?;

                LOCAL_OPS.with(|ops| {
                    if ops.borrow().len() < 2048 {
                        ops.borrow_mut().push(LocalOp::FindParentPtrCmp {
                            swip_ptr, latch_ptr
                        });
                    } else {
                        println!("full");
                    }
                });

                error::Result::Ok(())
            };

            let _ = try_store();
        }
    };
}

#[macro_export]
macro_rules! dbg_find_parent_step_inner {
    ($guard:expr, $pos:expr, $key:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            let try_store = || {
                use super::node::NodeKind;
                use crate::persistent::debug::{to_u64, LOCAL_OPS, LocalOp};
                if let NodeKind::Internal(ref internal) = $guard.downcast() {
                    let lower = to_u64(internal.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
                    let upper = to_u64(internal.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
                    let pos = $pos as u64;
                    let key = to_u64($key);

                    $guard.recheck()?;

                    LOCAL_OPS.with(|ops| {
                        if ops.borrow().len() < 2048 {
                            ops.borrow_mut().push(LocalOp::FindParentStepInner {
                                lower, upper, key, pos
                            });
                        } else {
                            println!("full");
                        }
                    });
                }
                error::Result::Ok(())
            };

            let _ = try_store();
        }
    };
}

#[macro_export]
macro_rules! dbg_find_parent_step_leaf {
    ($guard:expr, $key:expr) => {
        #[cfg(feature = "deepdebug")]
        {
            let try_store = || {
                use super::node::NodeKind;
                use crate::persistent::debug::{to_u64, LOCAL_OPS, LocalOp};
                if let NodeKind::Leaf(ref leaf) = $guard.downcast() {
                    let lower = to_u64(leaf.base.lower_fence()?.unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
                    let upper = to_u64(leaf.base.upper_fence()?.unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
                    let key = to_u64($key);

                    $guard.recheck()?;

                    LOCAL_OPS.with(|ops| {
                        if ops.borrow().len() < 2048 {
                            ops.borrow_mut().push(LocalOp::FindParentStepLeaf {
                                lower, upper, key
                            });
                        } else {
                            println!("full");
                        }
                    });
                }
                error::Result::Ok(())
            };

            let _ = try_store();
        }
    };
}

#[macro_export]
macro_rules! dbg_local_clear {
    () => {
        #[cfg(feature = "deepdebug")]
        {
            use crate::persistent::debug::LOCAL_OPS;
            LOCAL_OPS.with(|ops| {
                ops.borrow_mut().clear();
            });
        }
    };
}

#[macro_export]
macro_rules! dbg_local_report {
    () => {
        #[cfg(feature = "deepdebug")]
        {
            use crate::persistent::debug::LOCAL_OPS;
            LOCAL_OPS.with(|ops| {
                println!("{:?}", *ops.borrow());
            });
        }
    };
}

#[inline(never)]
pub(crate) fn print_dbg_reports(n: usize) -> usize {
    if n < 10 {
        dbg_global_report!();
        dbg_local_report!();
        n
    } else {
        0
    }
}
