use crate::tree::{PersistentBPlusTree, OptNodeGuard, ShrNodeGuard, ExvNodeGuard, Direction, swip_to_node_guard, bf_to_node_guard};
use bplustree::latch::{OptimisticGuard, SharedGuard, ExclusiveGuard};
use crate::error;

#[derive(Debug, PartialEq, Copy, Clone)]
enum Cursor {
    After(usize),
    Before(usize)
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum LeafResult {
    Ok,
    End
}

macro_rules! tp {
    ($x:expr) => {
        // println!("[{:?}] {}", std::thread::current().id(), format!($x));
    };
    ($x:expr, $($y:expr),+) => {
        // println!("[{:?}] {}", std::thread::current().id(), format!($x, $($y),+));
    };
}

/// Raw shared iterator over the entries of the tree.
pub struct RawSharedIter<'t> {
    tree: &'t PersistentBPlusTree,
    parent: Option<(OptNodeGuard, usize)>,
    leaf: Option<(ShrNodeGuard, Cursor)>,
    buffer: Vec<u8>
}

impl <'t> RawSharedIter<'t> {
    pub(crate) fn new(tree: &'t PersistentBPlusTree) -> RawSharedIter<'t> {
        RawSharedIter {
            tree,
            parent: None,
            leaf: None,
            buffer: vec!()
        }
    }

    fn next_leaf(&mut self) -> LeafResult {
        if let Some((guard, cursor)) = self.leaf.as_mut() {
            if let Some(key) = guard.upper_fence().expect("not optimistic") {
                let next_key = [key, &[0]].concat().to_vec();

                let _ = self.leaf.take(); // Drops leaf lock to try acquiring next (important)

                match self.optimistic_jump(Direction::Forward) {
                    Ok(res) => res,
                    Err(error::Error::Unwind) => {
                        self.seek(next_key);
                        LeafResult::Ok
                    }
                    Err(_e) => {
                        unreachable!("no other error expected");
                    }
                }
            } else {
                LeafResult::End
            }
        } else {
            LeafResult::End
        }
    }

    fn prev_leaf(&mut self) -> LeafResult {
        if let Some((guard, cursor)) = self.leaf.as_mut() {
            if let Some(key) = guard.lower_fence().expect("not optimistic") {
                let prev_key = key.to_vec();

                let _ = self.leaf.take(); // Drops leaf lock to try acquiring next (important)

                match self.optimistic_jump(Direction::Reverse) {
                    Ok(res) => res,
                    Err(error::Error::Unwind) => {
                        self.seek_for_prev(prev_key);
                        LeafResult::Ok
                    }
                    Err(_e) => {
                        unreachable!("no other error expected");
                    }
                }
            } else {
                LeafResult::End
            }
        } else {
            LeafResult::End
        }
    }

    fn optimistic_jump(&mut self, direction: Direction) -> error::Result<LeafResult> {
        if let Some((mut parent_guard, p_cursor)) = self.parent.take() {
            let bounded_pos = match direction {
                Direction::Forward if p_cursor < parent_guard.len() => Some(p_cursor + 1),
                Direction::Reverse if p_cursor > 0 => Some(p_cursor - 1),
                _ => None
            };
            if let Some(pos) = bounded_pos {
                let swip_guard = OptimisticGuard::map(parent_guard, |node| node.as_internal().edge_at(pos))?;
                let (swip_guard, guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                parent_guard = swip_to_node_guard(swip_guard);

                assert!(guard.is_leaf());

                let shared_guard = guard.to_shared()?;

                let l_cursor = match direction {
                    Direction::Forward => Cursor::Before(0),
                    Direction::Reverse => Cursor::After(shared_guard.len() - 1)
                };

                self.parent = Some((parent_guard, pos));
                self.leaf = Some((shared_guard, l_cursor));
                Ok(LeafResult::Ok)
            } else {
                Err(error::Error::Unwind) // This means the cursor reached the end of the parent, we need to find from root again
            }
        } else {
            Ok(LeafResult::End)
        }
    }

    pub fn leaf_for_key<K: AsRef<[u8]>>(&mut self, key: K) -> (ShrNodeGuard, Option<(OptNodeGuard, usize)>) {
        let key = key.as_ref();
        match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().within_bounds(key).expect("not optimistic") => {
                (guard, self.parent.take())
            }
            Some((guard, _)) => {
                drop(guard);
                self.parent.take();
                self.tree.find_shared_leaf_and_optimistic_parent(key)
            }
            _ => {
                self.parent.take();
                self.tree.find_shared_leaf_and_optimistic_parent(key)
            }
        }
    }

    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        let (guard, parent_opt) = self.leaf_for_key(key);

        let leaf = guard.as_leaf();
        let leaf_len = leaf.base.len();
        let (pos, _) = leaf.lower_bound(key).expect("not optimistic");
        if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        }
    }

    pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        let (guard, parent_opt) = self.leaf_for_key(key);

        let leaf = guard.as_leaf();
        let leaf_len = leaf.base.len();
        let (pos, exact) = leaf.lower_bound(key).expect("not optimistic");
        if exact {
            self.leaf = Some((guard, Cursor::After(pos)));
            self.parent = parent_opt;
        } else if pos == 0 {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        } else if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::After(pos)));
            self.parent = parent_opt;
        }
    }

    pub fn seek_exact<K: AsRef<[u8]>>(&mut self, key: K) -> bool {
        let key = key.as_ref();
        let (guard, parent_opt) = self.leaf_for_key(key);

        let leaf = guard.as_leaf();
        let leaf_len = leaf.base.len();
        let (pos, exact) = leaf.lower_bound(key).expect("not optimistic");
        if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        }

        exact
    }

    pub fn seek_to_first(&mut self) {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.lower_fence().expect("not optimistic").is_none() => {
                (guard, self.parent.take())
            }
            Some((guard, _)) => {
                drop(guard);
                self.parent.take();
                self.tree.find_first_shared_leaf_and_optimistic_parent()
            }
            None => {
                self.parent.take();
                self.tree.find_first_shared_leaf_and_optimistic_parent()
            }
        };

        self.leaf = Some((guard, Cursor::Before(0)));
        self.parent = parent_opt;
    }

    pub fn seek_to_last(&mut self) {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.upper_fence().expect("not optimistic").is_none() => {
                (guard, self.parent.take())
            }
            Some((guard, _)) => {
                drop(guard);
                self.parent.take();
                self.tree.find_last_shared_leaf_and_optimistic_parent()
            }
            _ => {
                self.parent.take();
                self.tree.find_last_shared_leaf_and_optimistic_parent()
            }
        };

        let leaf_len = guard.len();
        self.leaf = Some((guard, Cursor::Before(leaf_len)));
        self.parent = parent_opt;
    }

    #[inline]
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        loop {
            let opt = match self.leaf.as_ref() {
                Some((guard, cursor)) => {
                    let leaf = guard.as_leaf();
                    match *cursor {
                        Cursor::Before(pos) => {
                            if pos < leaf.base.len() {
                                Some((pos, Cursor::Before(pos + 1)))
                            } else {
                                None
                            }
                        },
                        Cursor::After(pos) => {
                            let curr_pos = pos + 1;
                            if curr_pos < leaf.base.len() {
                                Some((curr_pos, Cursor::Before(curr_pos + 1)))
                            } else {
                                None
                            }
                        }
                    }
                }
                None => {
                    // TODO seek to first?
                    return None;
                }
            };

            if let Some((curr_pos, new_cursor)) = opt {
                let (guard, cursor) = self.leaf.as_mut().unwrap();
                let leaf = guard.as_leaf();
                *cursor = new_cursor;
                leaf.copy_full_key_at(curr_pos, &mut self.buffer).expect("not optimistic");
                let value = leaf.value_at(curr_pos).expect("not optimistic");
                return Some((&self.buffer[..], value));
            } else {
                match self.next_leaf() {
                    LeafResult::Ok => {
                        // Try to fetch next from this new leaf
                        continue;
                    }
                    LeafResult::End => {
                        return None;
                    }
                }
            }
        }
    }

    #[inline]
    pub fn prev(&mut self) -> Option<(&[u8], &[u8])> {
        loop {
            let opt = match self.leaf.as_ref() {
                Some((_guard, cursor)) => {
                    match *cursor {
                        Cursor::After(pos) => {
                            if pos > 0 {
                                Some((pos, Cursor::After(pos - 1)))
                            } else if pos == 0 {
                                Some((pos, Cursor::Before(pos)))
                            } else {
                                None
                            }
                        },
                        Cursor::Before(pos) => {
                            if pos > 0 {
                                let curr_pos = pos - 1;
                                if curr_pos == 0 {
                                    Some((curr_pos, Cursor::Before(curr_pos)))
                                } else {
                                    Some((curr_pos, Cursor::After(curr_pos - 1)))
                                }
                            } else {
                                None
                            }
                        }
                    }
                }
                None => {
                    // TODO seek to last?
                    return None;
                }
            };

            if let Some((curr_pos, new_cursor)) = opt {
                let (guard, cursor) = self.leaf.as_mut().unwrap();
                let leaf = guard.as_leaf();
                *cursor = new_cursor;
                leaf.copy_full_key_at(curr_pos, &mut self.buffer).expect("not optimistic");
                let value = leaf.value_at(curr_pos).expect("not optimistic");
                return Some((&self.buffer[..], value));
            } else {
                match self.prev_leaf() {
                    LeafResult::Ok => {
                        // Try to fetch prev from this new leaf
                        continue;
                    }
                    LeafResult::End => {
                        return None;
                    }
                }
            }
        }
    }
}

/// Raw exclusive iterator over the entries of the tree.
pub struct RawExclusiveIter<'t> {
    tree: &'t PersistentBPlusTree,
    parent: Option<(OptNodeGuard, usize)>,
    leaf: Option<(ExvNodeGuard, Cursor)>,
    buffer: Vec<u8>
}

impl <'t> RawExclusiveIter<'t> {
    pub(crate) fn new(tree: &'t PersistentBPlusTree) -> RawExclusiveIter<'t> {
        RawExclusiveIter {
            tree,
            parent: None,
            leaf: None,
            buffer: vec!()
        }
    }

    fn next_leaf(&mut self) -> LeafResult {
        if let Some((guard, cursor)) = self.leaf.as_mut() {
            if let Some(key) = guard.upper_fence().expect("not optimistic") {
                let next_key = [key, &[0]].concat().to_vec();

                let _ = self.leaf.take(); // Drops leaf lock to try acquiring next (important)

                match self.optimistic_jump(Direction::Forward) {
                    Ok(res) => res,
                    Err(error::Error::Unwind) => {
                        self.seek(next_key);
                        LeafResult::Ok
                    }
                    Err(_e) => {
                        unreachable!("no other error expected");
                    }
                }
            } else {
                LeafResult::End
            }
        } else {
            LeafResult::End
        }
    }

    fn prev_leaf(&mut self) -> LeafResult {
        if let Some((guard, cursor)) = self.leaf.as_mut() {
            if let Some(key) = guard.lower_fence().expect("not optimistic") {
                let prev_key = key.to_vec();

                let _ = self.leaf.take(); // Drops leaf lock to try acquiring next (important)

                match self.optimistic_jump(Direction::Reverse) {
                    Ok(res) => res,
                    Err(error::Error::Unwind) => {
                        self.seek_for_prev(prev_key);
                        LeafResult::Ok
                    }
                    Err(_e) => {
                        unreachable!("no other error expected");
                    }
                }
            } else {
                LeafResult::End
            }
        } else {
            LeafResult::End
        }
    }

    fn optimistic_jump(&mut self, direction: Direction) -> error::Result<LeafResult> {
        if let Some((mut parent_guard, p_cursor)) = self.parent.take() {
            let bounded_pos = match direction {
                Direction::Forward if p_cursor < parent_guard.len() => Some(p_cursor + 1),
                Direction::Reverse if p_cursor > 0 => Some(p_cursor - 1),
                _ => None
            };
            if let Some(pos) = bounded_pos {
                let swip_guard = OptimisticGuard::map(parent_guard, |node| node.as_internal().edge_at(pos))?;
                let (swip_guard, guard) = PersistentBPlusTree::lock_coupling(swip_guard)?;
                parent_guard = swip_to_node_guard(swip_guard);

                assert!(guard.is_leaf());

                let exclusive_guard = guard.to_exclusive()?;

                let l_cursor = match direction {
                    Direction::Forward => Cursor::Before(0),
                    Direction::Reverse => Cursor::After(exclusive_guard.len() - 1)
                };

                self.parent = Some((parent_guard, pos));
                self.leaf = Some((exclusive_guard, l_cursor));
                Ok(LeafResult::Ok)
            } else {
                Err(error::Error::Unwind) // This means the cursor reached the end of the parent, we need to find from root again
            }
        } else {
            Ok(LeafResult::End)
        }
    }

    pub fn leaf_for_key<K: AsRef<[u8]>>(&mut self, key: K) -> (ExvNodeGuard, Option<(OptNodeGuard, usize)>) {
        let key = key.as_ref();
        match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().within_bounds(key).expect("not optimistic") => {
                (guard, self.parent.take())
            }
            Some((guard, _)) => {
                drop(guard);
                tp!("finding other leaf");
                self.parent.take();
                self.tree.find_exclusive_leaf_and_optimistic_parent(key)
            }
            None => {
                tp!("finding other leaf none");
                self.parent.take();
                self.tree.find_exclusive_leaf_and_optimistic_parent(key)
            }
        }
    }

    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        let (guard, parent_opt) = self.leaf_for_key(key);

        let leaf = guard.as_leaf();
        let leaf_len = leaf.base.len();
        let (pos, _) = leaf.lower_bound(key).expect("not optimistic");
        if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        }
    }

    pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        let (guard, parent_opt) = self.leaf_for_key(key);

        let leaf = guard.as_leaf();
        let leaf_len = leaf.base.len();
        let (pos, exact) = leaf.lower_bound(key).expect("not optimistic");
        if exact {
            self.leaf = Some((guard, Cursor::After(pos)));
            self.parent = parent_opt;
        } else if pos == 0 {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        } else if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::After(pos)));
            self.parent = parent_opt;
        }
    }

    pub fn seek_exact<K: AsRef<[u8]>>(&mut self, key: K) -> bool {
        let key = key.as_ref();
        let (guard, parent_opt) = self.leaf_for_key(key);
        tp!("seek exact done");

        let leaf = guard.as_leaf();
        let leaf_len = leaf.base.len();
        let (pos, exact) = leaf.lower_bound(key).expect("not optimistic");
        if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        }

        exact
    }

    pub fn seek_to_first(&mut self) {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.lower_fence().expect("not optimistic").is_none() => {
                (guard, self.parent.take())
            }
            Some((guard, _)) => {
                drop(guard);
                self.parent.take();
                self.tree.find_first_exclusive_leaf_and_optimistic_parent()
            }
            None => {
                self.parent.take();
                self.tree.find_first_exclusive_leaf_and_optimistic_parent()
            }
        };

        self.leaf = Some((guard, Cursor::Before(0)));
        self.parent = parent_opt;
    }

    pub fn seek_to_last(&mut self) {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.upper_fence().expect("not optimistic").is_none() => {
                (guard, self.parent.take())
            }
            Some((guard, _)) => {
                drop(guard);
                self.parent.take();
                self.tree.find_last_exclusive_leaf_and_optimistic_parent()
            }
            None => {
                self.parent.take();
                self.tree.find_last_exclusive_leaf_and_optimistic_parent()
            }
        };

        let leaf_len = guard.len();
        self.leaf = Some((guard, Cursor::Before(leaf_len)));
        self.parent = parent_opt;
    }

    #[inline]
    pub fn next(&mut self) -> Option<(&[u8], &mut [u8])> {
        loop {
            let opt = match self.leaf.as_ref() {
                Some((guard, cursor)) => {
                    let leaf = guard.as_leaf();
                    match *cursor {
                        Cursor::Before(pos) => {
                            if pos < leaf.base.len() {
                                Some((pos, Cursor::Before(pos + 1)))
                            } else {
                                None
                            }
                        },
                        Cursor::After(pos) => {
                            let curr_pos = pos + 1;
                            if curr_pos < leaf.base.len() {
                                Some((curr_pos, Cursor::Before(curr_pos + 1)))
                            } else {
                                None
                            }
                        }
                    }
                }
                None => {
                    // TODO seek to first?
                    return None;
                }
            };

            if let Some((curr_pos, new_cursor)) = opt {
                let (guard, cursor) = self.leaf.as_mut().unwrap();
                let leaf = guard.as_leaf_mut();
                *cursor = new_cursor;
                // println!("node: {:?}", leaf.base);
                // println!("lower_fence: {:?}", leaf.base.lower_fence().unwrap());
                // println!("upper_fence: {:?}", leaf.base.upper_fence().unwrap());
                // println!("curr_pos: {}", curr_pos);
                // println!("key: {:?}", leaf.key_at(curr_pos).unwrap());
                // println!("fullkey: {:?}", leaf.full_key_at(curr_pos).unwrap());
                leaf.copy_full_key_at(curr_pos, &mut self.buffer).expect("not optimistic");
                let value = leaf.value_at_mut(curr_pos);
                return Some((&self.buffer[..], value));
            } else {
                match self.next_leaf() {
                    LeafResult::Ok => {
                        // Try to fetch next from this new leaf
                        continue;
                    }
                    LeafResult::End => {
                        return None;
                    }
                }
            }
        }
    }

    #[inline]
    pub fn prev(&mut self) -> Option<(&[u8], &mut [u8])> {
        loop {
            let opt = match self.leaf.as_ref() {
                Some((_guard, cursor)) => {
                    match *cursor {
                        Cursor::After(pos) => {
                            if pos > 0 {
                                Some((pos, Cursor::After(pos - 1)))
                            } else if pos == 0 {
                                Some((pos, Cursor::Before(pos)))
                            } else {
                                None
                            }
                        },
                        Cursor::Before(pos) => {
                            if pos > 0 {
                                let curr_pos = pos - 1;
                                if curr_pos == 0 {
                                    Some((curr_pos, Cursor::Before(curr_pos)))
                                } else {
                                    Some((curr_pos, Cursor::After(curr_pos - 1)))
                                }
                            } else {
                                None
                            }
                        }
                    }
                }
                None => {
                    // TODO seek to last?
                    return None;
                }
            };

            if let Some((curr_pos, new_cursor)) = opt {
                let (guard, cursor) = self.leaf.as_mut().unwrap();
                let leaf = guard.as_leaf_mut();
                *cursor = new_cursor;
                leaf.copy_full_key_at(curr_pos, &mut self.buffer).expect("not optimistic");
                let value = leaf.value_at_mut(curr_pos);
                return Some((&self.buffer[..], value));
            } else {
                match self.prev_leaf() {
                    LeafResult::Ok => {
                        // Try to fetch prev from this new leaf
                        continue;
                    }
                    LeafResult::End => {
                        return None;
                    }
                }
            }
        }
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        let key = key.as_ref();
        let value = value.as_ref();
        'start: loop {
            if self.seek_exact(key) {
                let leaf = self.leaf.as_mut().expect("seeked").0.as_leaf_mut();
                let (pos, exact) = leaf.lower_bound(key).expect("not optimistic");
                let payload = leaf.value_at_mut(pos);
                if payload.len() == value.len() { // TODO optimize for <= value.len() (shorten_payload)
                    payload.copy_from_slice(value);
                    break;
                } else {
                    leaf.remove_at(pos);
                    continue 'start;
                }
                // println!("{:?}", leaf.base);
                // println!("{:?} {:?}", key, leaf.full_key_at(pos).expect("not optimistic").as_slice());
                // todo!("replace");
                // let (_k, v) = self.next().unwrap();
                // let old = std::mem::replace(v, value);
                // break Some(old);
            } else {
                let (guard, cursor) = self.leaf.as_mut().expect("just seeked");
                if guard.as_leaf().can_insert(key.len(), value.len()) {
                    tp!("inserting");
                    let leaf = guard.as_leaf_mut();
                    match *cursor {
                        Cursor::Before(pos) => {
                            leaf.insert_at(pos, key, value).expect("just checked for space");
                            tp!("done inserting");
                        }
                        Cursor::After(_) => {
                            unreachable!("seek_exact always sets cursor to before");
                        }
                    }
                    break;
                } else {
                    tp!("cant insert");
                    self.parent.take();
                    let (guard, _cursor) = self.leaf.take().expect("just seeked");
                    let mut guard = guard.unlock();

                    loop {
                        let perform_split = || {
                            if !guard.as_leaf().can_insert(key.len(), value.len()) {
                                guard.recheck()?;
                                self.tree.try_split(&guard)?;
                            }
                            error::Result::Ok(())
                        };

                        match perform_split() {
                            Ok(_) => break,
                            Err(error::Error::Reclaimed) => {
                                unreachable!("can happen?");
                                continue 'start;
                            }
                            Err(_) => {
                                guard = bf_to_node_guard(guard.latch().optimistic_or_spin());
                                continue
                            }
                        }
                    }

                    tp!("split done");
                    // Split complete, try again
                    continue;
                }
            }
        }
    }

    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) {
        if self.seek_exact(key) {
            self.remove_next()
        } else {
            // None
        }
    }

    fn remove_next(&mut self) {
        match self.leaf.as_mut() {
            Some((guard, cursor)) => {
                let leaf = guard.as_leaf_mut();

                let removed = match *cursor {
                    Cursor::Before(pos) => {
                        let curr_pos = pos;
                        if curr_pos < leaf.base.len() {
                            leaf.copy_full_key_at(curr_pos, &mut self.buffer).expect("not optimistic");
                            leaf.remove_at(curr_pos);
                            Some(&self.buffer[..])
                        } else {
                            None
                        }
                    }
                    Cursor::After(pos) => {
                        let curr_pos = pos + 1;
                        if curr_pos < leaf.base.len() {
                            leaf.copy_full_key_at(curr_pos, &mut self.buffer).expect("not optimistic");
                            leaf.remove_at(curr_pos);
                            Some(&self.buffer[..])
                        } else {
                            None
                        }
                    }
                };

                if let Some(removed_key) = removed.as_ref() {
                    if guard.is_underfull() {
                        self.parent.take();
                        let (guard, _cursor) = self.leaf.take().expect("just seeked");

                        let guard = guard.unlock();
                        loop {
                            let perform_merge = || {
                                let _ = self.tree.try_merge(&guard)?;
                                error::Result::Ok(())
                            };

                            match perform_merge() {
                                Ok(_) => {
                                    break;
                                },
                                Err(error::Error::Reclaimed) => {
                                    unreachable!("can happen?");
                                    break;
                                }
                                Err(_) => {
                                    break; // TODO not ensuring merges
                                    // guard = guard.latch().optimistic_or_spin();
                                    // continue
                                }
                            }
                        }

                        self.seek(removed_key.to_vec());
                    }
                }

                // removed
            }
            None => {
                // None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tree::{PersistentBPlusTree};
    use crate::ensure_global_bufmgr;

    use super::{RawExclusiveIter};
    use serial_test::serial;

    macro_rules! kv {
        ($n:expr) => {
            Some((&$n[..], &$n[..]))
        };
    }

    macro_rules! kv_mut {
        ($n:expr) => {
            Some((&$n[..], &mut $n[..]))
        };
    }

    #[test]
    #[serial]
    fn persistent_iter_test() {
        ensure_global_bufmgr("/tmp/state.db", 1 * 1024 * 1024).unwrap();
        let tree = PersistentBPlusTree::new();
        let mut iter = RawExclusiveIter::new(&tree);
        iter.insert([1], [1]);
        iter.seek_to_first();
        assert_eq!(iter.next(), kv_mut!([1]));
        assert_eq!(iter.prev(), kv_mut!([1]));
        iter.remove([1]);

        iter.seek_to_first();
        assert_eq!(iter.next(), None);
    }

    // FIXME !!!!!!!!!!!! writing past capacity
    #[test]
    #[serial]
    fn persistent_sanity_check() {
        use rand::thread_rng;
        use rand::seq::SliceRandom;

        ensure_global_bufmgr("/tmp/state.db", 100 * 1024 * 1024).unwrap();

        let bptree = PersistentBPlusTree::new();

        let mut iter = RawExclusiveIter::new(&bptree);

        let n = 1000000usize;

        let mut data: Vec<_> = (0..n).collect();

        data.shuffle(&mut thread_rng());

        let mut strings = vec!();

        for i in 0..n {
            strings.push(format!("{:09}", data[i]).into_bytes());
        }

        let mut sorted_data = data.clone();
        sorted_data.sort();
        let mut sorted_strings = strings.clone();
        sorted_strings.sort();


        println!("PersistentBPlusTree");

        let t0 = std::time::Instant::now();
        for i in 0..n {
            iter.insert(strings[i].as_slice(), data[i].to_be_bytes());
            // println!("n: {}", i);
        }
        println!("insert took: {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        iter.seek_to_first();
        println!("lookup took: {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        for i in 0..n {
            let mut datum = sorted_data[i].to_be_bytes();
//             let a = iter.next();
//             let b = Some((sorted_strings[i].as_slice(), &mut datum[..]));
//             if a != b {
//                 println!("{:?} != {:?}", a, b);
//                 iter.leaf.as_ref().map(|(l, c)| {
//                     let leaf = l.as_leaf();
//                     for i in 0..l.len() {
//                         println!("{:?}", leaf.key_at(i).unwrap());
//                     }
//                     println!("sorted {:?}", leaf.is_sorted());
//                 });
//                 panic!("wrong");
//             }
            assert_eq!(iter.next(), Some((sorted_strings[i].as_slice(), &mut datum[..])));
            // println!("n: {}", i);
        }
        assert_eq!(iter.next(), None);

        println!("scan took: {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        for i in (0..n).rev() {
            iter.remove(strings[i].as_slice());
        }
        println!("remove took: {:?}", t0.elapsed());

        // let found = bptree.lookup("009999", |value| *value); // Caution, using this while holding the iterator may deadlock;
        // assert_eq!(found, None);

        let t0 = std::time::Instant::now();
        let mut count = 0;
        iter.seek_to_first();
        while let Some(_) = iter.next() {
            count += 1;
        }
        println!("empty count took: {:?}", t0.elapsed());

        assert_eq!(count, 0);

        println!("BTreeMap");

        use std::collections::BTreeMap;

        let mut btreemap: BTreeMap<Vec<u8>, usize> = BTreeMap::default();

        let t0 = std::time::Instant::now();
        for i in 0..n {
            btreemap.insert(strings[i].clone(), data[i]);
        }
        println!("insert took: {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        btreemap.contains_key(&b""[..]);
        println!("lookup took: {:?}", t0.elapsed());

        {
            let mut iter = btreemap.iter_mut();
            let t0 = std::time::Instant::now();
            for i in 0..n {
                assert_eq!(iter.next(), Some((&sorted_strings[i], &mut sorted_data[i])));
            }

            assert_eq!(iter.next(), None);

            println!("scan took: {:?}", t0.elapsed());
        }

        let t0 = std::time::Instant::now();
        for i in (0..n).rev() {
            assert_eq!(btreemap.remove(&strings[i]).as_ref(), Some(&data[i]));
        }
        println!("remove took: {:?}", t0.elapsed());

        {
            let t0 = std::time::Instant::now();
            let mut count = 0;
            let mut iter = btreemap.iter();
            while let Some(_) = iter.next() {
                count += 1;
            }
            println!("empty count took: {:?}", t0.elapsed());

            assert_eq!(count, 0);
        }
    }
}
