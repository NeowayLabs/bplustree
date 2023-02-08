use std::sync::atomic::Ordering;
use std::collections::HashMap;

use super::{
    BufferManager,
    BufferFrame,
    BfState,
    WriteBuffer,
    write_buffer::LatchOrGuard,
    RefOrPid,
    ParentResult
};

use crate::{latch::{OptimisticGuard, HybridLatch}, persistent::bufmgr::{EMPTY_EPOCH, RESERVED_EPOCH}};

use crate::error;

impl BufferManager {
    pub fn page_provider_epoch(&'static self) {
        let mut map: HashMap<_, _> = self.classes.iter()
            .map(|sc| {
                let n_slots = ((4 * 1024 * 1024) as f64 / sc.class_size() as f64).max(2.0) as usize;
                (sc.class, WriteBuffer::new(&self.fd, n_slots, sc.class_size()))
            })
            .collect();

        struct ClassMeta {
            write_buffer: WriteBuffer,
            offset: usize,
            needs_sampling: bool,
            eviction_window: usize,
            saved_free_count: usize,
            evicted_count: usize,
            sample_epochs: Vec<usize>,
            candidate_batch: Vec<(usize, &'static HybridLatch<BufferFrame>)>
        }

        let mut class_meta: Vec<ClassMeta> = self.classes.iter()
            .map(|sc| {
                let n_slots = ((4 * 1024 * 1024) as f64 / sc.class_size() as f64).max(2.0) as usize;
                let write_buffer = WriteBuffer::new(&self.fd, n_slots, sc.class_size());
                let n_required_samples = 600.min((sc.frames.len() / 2).max(1));
                ClassMeta {
                    write_buffer,
                    offset: 0,
                    needs_sampling: true,
                    eviction_window: 0,
                    saved_free_count: sc.free_frames.len(),
                    evicted_count: 0,
                    sample_epochs: vec![0usize; n_required_samples],
                    candidate_batch: Vec::with_capacity(128),
                }
            })
            .collect();

        let mut rng = rand::thread_rng();

        while self.running.load(Ordering::Acquire) {
            for (sc_idx, size_class) in self.classes.iter().enumerate() {
                if size_class.frames.len() == 0 {
                    continue;
                }

                let ClassMeta {
                    write_buffer,
                    offset,
                    needs_sampling,
                    eviction_window,
                    saved_free_count,
                    evicted_count,
                    sample_epochs,
                    candidate_batch,
                } = class_meta.get_mut(sc_idx).expect("must exist");

                let free_lower_bound = (size_class.frames.len() as f64  * 0.10).ceil() as usize;
                let needs_eviction = || size_class.free_frames.len() < free_lower_bound;
                let cool_lower_bound = (size_class.frames.len() as f64 * 0.20).ceil() as usize;
                let needs_cooling = || size_class.free_frames.len() < cool_lower_bound;

                let eviction_churn = 0.1; // adaptive: (1.0 - size_class.free_frames.len().min(free_lower_bound) as f64 / free_lower_bound as f64).min(0.8);

                let needs_epoch_inc = |free_count: usize, evicted: usize| {
                    (size_class.free_frames.len() + (free_lower_bound / 10)) < (free_count + evicted) || size_class.free_frames.len() == 0
                };

                let batch_size = 128;
                let n_required_samples = sample_epochs.len();


                let evict_frame = |guard: OptimisticGuard<'static, BufferFrame>| {
                    let dtid = guard.page.dtid;
                    guard.recheck()?;

                    let (result, guard) = self.catalog().get(dtid).expect("exists").find_parent(guard)?;
                    match result {
                        ParentResult::Root => {
                            unreachable!("cannot evict root");
                        }
                        ParentResult::Parent(swip_guard) => {
                            let mut swip_x_guard = swip_guard.try_to_exclusive()?;
                            let mut frame_x_guard = guard.try_to_exclusive()?;

                            crate::dbg_evict_out!(self, swip_x_guard, &frame_x_guard);

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

                if needs_cooling() {
                    if needs_epoch_inc(*saved_free_count, *evicted_count) {
                        *evicted_count = 0;
                        self.global_epoch.fetch_add(1, Ordering::AcqRel);
                        *saved_free_count = size_class.free_frames.len();
                        *needs_sampling = true;
                    }


                    if needs_eviction() {
                        let begin = *offset;
                        let mut end = *offset + batch_size;
                        if end > size_class.frames.len() {
                            *offset = 0;
                            end = size_class.frames.len();
                        } else {
                            *offset = end;
                        }

                        if *needs_sampling {
                            let mut n_samples = 0;
                            let mut n_tries = 0;

                            let mut sample_offset = begin; // TODO check if we should really reset here
                            while n_samples < n_required_samples && n_tries < n_required_samples * 5 { // TODO tune
                                let begin = sample_offset;
                                let mut end = sample_offset + batch_size;
                                if end > size_class.frames.len() {
                                    sample_offset = 0;
                                    end = size_class.frames.len();
                                } else {
                                    sample_offset = end;
                                }

                                for latch in size_class.frames[begin..end].iter() {
                                    if n_samples == n_required_samples { break; }

                                    n_tries += 1;

                                    let epoch = unsafe { (*latch.data_ptr()).epoch.load(Ordering::Acquire) };

                                    if epoch == EMPTY_EPOCH || epoch == RESERVED_EPOCH { continue; }

                                    sample_epochs[n_samples] = epoch;
                                    n_samples += 1;
                                }
                            }

                            if n_samples > 0 {
                                sample_epochs[..n_samples].sort();
                                let eviction_idx = (n_samples as f64 * eviction_churn).floor() as usize;
                                *eviction_window = sample_epochs[eviction_idx.min(n_samples - 1)];
                                *needs_sampling = false;
                            } else {
                                // Could not sample anything, setting a low eviction window
                                *eviction_window = 0;
                            }
                        }

                        for latch in size_class.frames[begin..end].iter() {
                            let frame = match latch.optimistic_or_unwind() {
                                Ok(f) => f,
                                Err(_) => continue,
                            };

                            let state = frame.state;
                            let epoch = frame.epoch.load(Ordering::Acquire); // frame.epoch;
                            let pid = frame.pid;

                            if frame.recheck().is_err() { continue; }

                            if state == BfState::Free
                                || state == BfState::Reclaim
                                || state == BfState::Loaded { continue; } // TODO check states

                            if pid.is_invalid() || latch.is_exclusively_latched() { continue; }

                            if epoch <= *eviction_window {
                                candidate_batch.push((epoch, latch));
                            } else {
                            }
                        }

                        while let Some((saved_epoch, latch)) = candidate_batch.pop() {
                            let frame = match latch.optimistic_or_unwind() {
                                Ok(f) => f,
                                Err(_) => continue,
                            };

                            let state = frame.state;
                            let epoch = frame.epoch.load(Ordering::Acquire);
                            let pid = frame.pid;
                            let dtid = frame.page.dtid;
                            let persisting = frame.persisting.load(Ordering::Acquire);

                            if frame.recheck().is_err() { continue; }

                            if saved_epoch != epoch { continue; }

                            if state == BfState::Free
                                || state == BfState::Reclaim
                                || state == BfState::Loaded { continue; } // TODO check states

                            if pid.is_invalid() { continue; }

                            if persisting { continue; }

                            // TODO verify if we should move this check to the earlier pass
                            let mut all_evicted = true;
                            let mut picked_child = false;

                            if self.catalog().get(dtid).expect("exists").iterate_children_swips(&frame, Box::new(|swip| {
                                match swip.try_downcast()? {
                                    RefOrPid::Pid(_) => {
                                        frame.recheck()?;
                                        Ok(true)
                                    },
                                    RefOrPid::Ref(r) => {
                                        let child = r.optimistic_or_unwind()?;
                                        frame.recheck()?;

                                        all_evicted = false;

                                        if child.page.size_class == frame.page.size_class {
                                            let epoch = child.epoch.load(Ordering::Acquire);
                                            child.recheck()?;
                                            candidate_batch.push((epoch, r));
                                            picked_child = true;
                                            Ok(false)
                                        } else {
                                            frame.recheck()?;
                                            Ok(false)
                                        }
                                    }
                                }
                            })).is_err() { continue; }

                            if !all_evicted || picked_child { continue; };

                                // TODO verify if we should check the io_map here

                            if frame.is_dirty() {
                                if write_buffer.is_full() {
                                    break;
                                } else {
                                    let mut frame_x = match frame.try_to_exclusive() {
                                        Ok(f) => f,
                                        Err(_) => continue,
                                    };

                                    debug_assert_ne!(BfState::Free, frame_x.state);
                                    debug_assert_ne!(BfState::Reclaim, frame_x.state);
                                    debug_assert_ne!(BfState::Loaded, frame_x.state);

                                    debug_assert_eq!(false, frame_x.persisting.load(Ordering::Acquire));
                                    frame_x.persisting.store(true, Ordering::Release); // TODO maybe try to exchange?

                                    // TODO crc

                                    // TODO out of place

                                    write_buffer.add(frame_x.downgrade());

                                    continue;
                                }
                            } else {
                                if let Err(_) = evict_frame(frame) {
                                    continue;
                                }
                                *evicted_count += 1;
                            }
                        }

                        write_buffer.submit();

                        let _polled_events = write_buffer.poll_events_sync();

                        for (latch_or_guard, written_gsn) in write_buffer.done_items() {
                            let latch = match latch_or_guard {
                                LatchOrGuard::Latch(latch) => latch,
                                LatchOrGuard::Guard(guard) => {
                                    let latch = guard.latch();
                                    let _ = guard.unlock();
                                    latch
                                }
                            };

                            let mut guard = match latch.try_exclusive() {
                                Some(g) => g,
                                None => {
                                    // When we cannot latch we should waste the write and move to
                                    // the next page, some thread may be stuck trying to allocate while
                                    // holding this lock

                                    // TODO atomic store: writting = false, crc = 0
                                    unsafe { (*latch.data_ptr()).persisting.store(false, Ordering::Release) } // TODO maybe exchange?
                                    continue;
                                }
                            };

                            // TODO check?: epoch_added != frame.epoch ? continue

                            assert!(guard.persisting.load(Ordering::Acquire));
                            assert!(guard.last_written_gsn < written_gsn);

                            // TODO out of place

                            guard.last_written_gsn = written_gsn;
                            guard.persisting.store(false, Ordering::Release);

                            if guard.state == BfState::Reclaim {
                                size_class.reclaim_page(guard);
                            } else if !guard.is_dirty() {
                                if let Err(_) = evict_frame(guard.unlock()) {
                                    continue;
                                }
                            }

                            *evicted_count += 1;
                        }
                    }
                }
            }
        }
    }
}
