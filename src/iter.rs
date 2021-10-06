use crate::{GenericBPlusTree, Node, Direction};
use crate::latch::{SharedGuard, ExclusiveGuard, OptimisticGuard};
use crossbeam_epoch::{self as epoch};
use crate::error;
use std::borrow::Borrow;

#[derive(Debug, PartialEq, Copy, Clone)]
enum Anchor<T> {
    Start,
    After(T),
    Before(T),
    End
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum Cursor {
    After(u16),
    Before(u16)
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum LeafResult {
    Ok,
    End,
    Retry
}

#[derive(Debug)]
enum JumpResult {
    Ok,
    End,
    Err(error::Error)
}

macro_rules! tp {
    ($x:expr) => {
        println!("[{:?}] {}", std::thread::current().id(), format!($x));
    };
    ($x:expr, $($y:expr),+) => {
        println!("[{:?}] {}", std::thread::current().id(), format!($x, $($y),+));
    };
}

pub struct RawSharedIter<'t, K, V, const IC: usize, const LC: usize> {
    tree: &'t GenericBPlusTree<K, V, IC, LC>,
    eg: epoch::Guard,
    parent: Option<(OptimisticGuard<'t, Node<K, V, IC, LC>>, u16)>,
    leaf: Option<(SharedGuard<'t, Node<K, V, IC, LC>>, Cursor)>
}

impl <'t, K: Clone + Ord, V, const IC: usize, const LC: usize> RawSharedIter<'t, K, V, IC, LC> {
    pub fn new(tree: &'t GenericBPlusTree<K, V, IC, LC>) -> RawSharedIter<'t, K, V, IC, LC> {
        RawSharedIter {
            tree,
            eg: epoch::pin(),
            parent: None,
            leaf: None
        }
    }

    #[inline]
    fn leaf_lt<'g>(guard: SharedGuard<'g, Node<K, V, IC, LC>>) -> SharedGuard<'t, Node<K, V, IC, LC>> {
        // Safety: We hold the epoch guard at all times so 'g should equal 't
        unsafe { std::mem::transmute(guard) }
    }
    #[inline]
    fn parent_lt<'g>(guard: OptimisticGuard<'g, Node<K, V, IC, LC>>) -> OptimisticGuard<'t, Node<K, V, IC, LC>> {
        // Safety: We hold the epoch guard at all times so 'g should equal 't
        unsafe { std::mem::transmute(guard) }
    }

    fn current_anchor(&self) -> Option<Anchor<K>> {
        if let Some((guard, cursor)) = self.leaf.as_ref() {
            let leaf = guard.as_leaf();
            let anchor = match *cursor {
                Cursor::Before(pos) => {
                    if pos >= leaf.len {
                        if leaf.len > 0 {
                            Anchor::After(leaf.key_at(leaf.len - 1).expect("should exist").clone())
                        } else {
                            if let Some(k) = &leaf.lower_fence {
                                Anchor::After(k.clone())
                            } else {
                                Anchor::Start
                            }
                        }
                    } else {
                        Anchor::Before(leaf.key_at(pos).expect("should exist").clone())
                    }
                }
                Cursor::After(pos) => {
                    if pos >= leaf.len {
                        if leaf.len > 0 {
                            Anchor::After(leaf.key_at(leaf.len - 1).expect("should exist").clone())
                        } else {
                            if let Some(k) = &leaf.upper_fence {
                                Anchor::After(k.clone())
                            } else {
                                Anchor::End
                            }
                        }
                    } else {
                        Anchor::After(leaf.key_at(pos).expect("should exist").clone())
                    }
                }
            };

            Some(anchor)
        } else {
            None
        }
    }

    fn restore_from_anchor(&mut self, anchor: &Anchor<K>) {
        self.parent.take();
        self.leaf.take(); // Make sure there are no locks held

        match anchor {
            Anchor::Start => {
                self.seek_to_first()
            }
            Anchor::End => {
                self.seek_to_last()
            }
            Anchor::Before(key) => {
                self.seek(key)
            }
            Anchor::After(key) => {
                self.seek_for_prev(key)
            }
        }
    }

    // Reimplementation of next_leaf and prev_leaf, previous implementation deadlocks (we cannot
    // try to lock a second leaf node without releasing the first)
    fn next_leaf(&mut self) -> LeafResult {
        if self.leaf.is_none() {
            return LeafResult::End;
        }

        let anchor = self.current_anchor().expect("leaf exists");

        loop {
            {
                let (guard, cursor) = self.leaf.as_ref().unwrap();
                let leaf = guard.as_leaf();
                match *cursor {
                    Cursor::Before(pos) if pos < leaf.len => {
                        // There is data on the leaf still
                        return LeafResult::Retry;
                    }
                    Cursor::After(pos) if (pos + 1) < leaf.len => {
                        // There is data on the leaf still
                        return LeafResult::Retry;
                    }
                    _ => {}
                }

                if leaf.upper_fence.is_none() || self.parent.is_none() {
                    // Leaf is root or there is no more data and no upper fence
                    return LeafResult::End;
                }
            }

            let _ = self.leaf.take(); // Drops leaf lock to try acquiring next (important)

            match self.optimistic_jump(Direction::Forward) {
                JumpResult::Ok => {
                    return LeafResult::Ok;
                }
                JumpResult::End => {
                    self.restore_from_anchor(&anchor);
                    continue;
                }
                JumpResult::Err(_) => {
                    self.restore_from_anchor(&anchor);
                    continue;
                }
            }
        }
    }

    fn prev_leaf(&mut self) -> LeafResult {
        if self.leaf.is_none() {
            return LeafResult::End;
        }

        let anchor = self.current_anchor().expect("leaf exists");

        loop {
            {
                let (guard, cursor) = self.leaf.as_ref().unwrap();
                let leaf = guard.as_leaf();
                match *cursor {
                    Cursor::Before(pos) if pos > 0 => {
                        // There is data on the leaf still
                        return LeafResult::Retry;
                    }
                    Cursor::After(_pos) => {
                        // There is data on the leaf still
                        return LeafResult::Retry;
                    }
                    _ => {}
                }

                if leaf.lower_fence.is_none() || self.parent.is_none() {
                    // Leaf is root or there is no more data and no lower fence
                    return LeafResult::End;
                }
            }

            let _ = self.leaf.take(); // Drops leaf lock to try acquiring next

            match self.optimistic_jump(Direction::Reverse) {
                JumpResult::Ok => {
                    return LeafResult::Ok;
                }
                JumpResult::End => {
                    self.restore_from_anchor(&anchor);
                    continue;
                }
                JumpResult::Err(_) => {
                    self.restore_from_anchor(&anchor);
                    continue;
                }
            }
        }
    }

    fn optimistic_jump(&mut self, direction: Direction) -> JumpResult {
        enum Outcome<L, P> {
            Leaf(L, u16),
            LeafAndParent(L, P, u16),
            End
        }

        let optmistic_perform = || {
            if let Some((parent_guard, p_cursor)) = self.parent.as_ref() {
                let bounded_pos = match direction {
                    Direction::Forward if *p_cursor < parent_guard.as_internal().len => Some(*p_cursor + 1),
                    Direction::Reverse if *p_cursor > 0 => Some(*p_cursor - 1),
                    _ => None
                };
                if let Some(pos) = bounded_pos {
                    let guard = {
                        let swip = parent_guard.as_internal().edge_at(pos)?;
                        GenericBPlusTree::lock_coupling(parent_guard, swip, &self.eg)?
                    };

                    assert!(guard.is_leaf());

                    error::Result::Ok(Outcome::Leaf(Self::leaf_lt(guard.to_shared()?), pos))
                } else {
                    let opt = match self.tree.find_nearest_leaf(parent_guard, direction, &self.eg)? { // TODO check if this is garanteed to return a sibling leaf
                        Some((guard, (parent, pos))) => {
                            Outcome::LeafAndParent(Self::leaf_lt(guard.to_shared()?), Self::parent_lt(parent), pos)
                        },
                        None => Outcome::End
                    };
                    error::Result::Ok(opt)
                }
            } else {
                error::Result::Ok(Outcome::End)
            }
        };


        match optmistic_perform() {
            Ok(Outcome::Leaf(leaf_guard, p_cursor)) => {
                let l_cursor = match direction {
                    Direction::Forward => Cursor::Before(0),
                    Direction::Reverse => Cursor::After(leaf_guard.as_leaf().len - 1)
                };

                self.leaf = Some((leaf_guard, l_cursor));
                self.parent.as_mut().map(|(_, p_c)| *p_c = p_cursor);

                return JumpResult::Ok;
            }
            Ok(Outcome::LeafAndParent(leaf_guard, parent_guard, p_cursor)) => {
                let l_cursor = match direction {
                    Direction::Forward => Cursor::Before(0),
                    Direction::Reverse => Cursor::After(leaf_guard.as_leaf().len - 1)
                };

                self.leaf = Some((leaf_guard, l_cursor));
                self.parent = Some((parent_guard, p_cursor));

                return JumpResult::Ok;
            }
            Ok(Outcome::End) => {
                return JumpResult::End;
            }
            Err(e) => {
                return JumpResult::Err(e);
            }
        }
    }

    pub fn seek<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().within_bounds(key) => {
                (guard, self.parent.take())
            }
            _ => {
                let (guard, parent_opt) = self.tree.find_shared_leaf_and_optimistic_parent(key, &self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        let leaf = guard.as_leaf();
        let leaf_len = leaf.len;
        let (pos, _) = leaf.lower_bound(key);
        if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        }
    }

    pub fn seek_for_prev<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().within_bounds(key) => {
                (guard, self.parent.take())
            }
            _ => {
                let (guard, parent_opt) = self.tree.find_shared_leaf_and_optimistic_parent(key, &self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        let leaf = guard.as_leaf();
        let leaf_len = leaf.len;
        let (pos, exact) = leaf.lower_bound(key);
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

    pub fn seek_exact<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().within_bounds(key) => {
                (guard, self.parent.take())
            }
            _ => {
                let (guard, parent_opt) = self.tree.find_shared_leaf_and_optimistic_parent(key, &self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        let leaf = guard.as_leaf();
        let leaf_len = leaf.len;
        let (pos, exact) = leaf.lower_bound(key);
        if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        }

        return exact;
    }

    pub fn seek_to_first(&mut self) {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().lower_fence.is_none() => {
                (guard, self.parent.take())
            }
            _ => {
                self.parent.take();
                let (guard, parent_opt) = self.tree.find_first_shared_leaf_and_optimistic_parent(&self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        self.leaf = Some((guard, Cursor::Before(0)));
        self.parent = parent_opt;
    }

    pub fn seek_to_last(&mut self) {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().upper_fence.is_none() => {
                (guard, self.parent.take())
            }
            _ => {
                self.parent.take();
                let (guard, parent_opt) = self.tree.find_last_shared_leaf_and_optimistic_parent(&self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        let leaf_len = guard.as_leaf().len;
        self.leaf = Some((guard, Cursor::Before(leaf_len)));
        self.parent = parent_opt;
    }

    #[inline]
    pub fn next(&mut self) -> Option<(&K, &V)> {
        loop {
            let opt = match self.leaf.as_ref() {
                Some((guard, cursor)) => {
                    let leaf = guard.as_leaf();
                    match *cursor {
                        Cursor::Before(pos) => {
                            if pos < leaf.len {
                                Some((pos, Cursor::Before(pos + 1)))
                            } else {
                                None
                            }
                        },
                        Cursor::After(pos) => {
                            let curr_pos = pos + 1;
                            if curr_pos < leaf.len {
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
                return Some(leaf.kv_at(curr_pos).expect("should exist"));
            } else {
                match self.next_leaf() {
                    LeafResult::Ok => {
                        // Try to fetch next from this new leaf
                        continue;
                    }
                    LeafResult::Retry => {
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
    pub fn prev(&mut self) -> Option<(&K, &V)> {
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
                return Some(leaf.kv_at(curr_pos).expect("should exist"));
            } else {
                match self.prev_leaf() {
                    LeafResult::Ok => {
                        // Try to fetch prev from this new leaf
                        continue;
                    }
                    LeafResult::Retry => {
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

// fn is_sorted<K: Ord>(slice: &[K]) -> bool {
//     for i in 0..slice.len() {
//         if i + 1 >= slice.len() {
//             break;
//         }
//
//         if slice[i] > slice[i + 1] {
//             return false;
//         }
//     }
//     true
// }

pub struct RawExclusiveIter<'t, K, V, const IC: usize, const LC: usize> {
    tree: &'t GenericBPlusTree<K, V, IC, LC>,
    eg: epoch::Guard,
    parent: Option<(OptimisticGuard<'t, Node<K, V, IC, LC>>, u16)>,
    leaf: Option<(ExclusiveGuard<'t, Node<K, V, IC, LC>>, Cursor)>
}

impl <'t, K: Clone + Ord, V, const IC: usize, const LC: usize> RawExclusiveIter<'t, K, V, IC, LC> {
    pub fn new(tree: &'t GenericBPlusTree<K, V, IC, LC>) -> RawExclusiveIter<'t, K, V, IC, LC> {
        RawExclusiveIter {
            tree,
            eg: epoch::pin(),
            parent: None,
            leaf: None
        }
    }

    #[inline]
    fn leaf_lt<'g>(guard: ExclusiveGuard<'g, Node<K, V, IC, LC>>) -> ExclusiveGuard<'t, Node<K, V, IC, LC>> {
        // Safety: We hold the epoch guard at all times so 'g should equal 't
        unsafe { std::mem::transmute(guard) }
    }
    #[inline]
    fn parent_lt<'g>(guard: OptimisticGuard<'g, Node<K, V, IC, LC>>) -> OptimisticGuard<'t, Node<K, V, IC, LC>> {
        // Safety: We hold the epoch guard at all times so 'g should equal 't
        unsafe { std::mem::transmute(guard) }
    }

    fn current_anchor(&self) -> Option<Anchor<K>> {
        if let Some((guard, cursor)) = self.leaf.as_ref() {
            let leaf = guard.as_leaf();
            let anchor = match *cursor {
                Cursor::Before(pos) => {
                    if pos >= leaf.len {
                        if leaf.len > 0 {
                            Anchor::After(leaf.key_at(leaf.len - 1).expect("should exist").clone())
                        } else {
                            if let Some(k) = &leaf.lower_fence {
                                Anchor::After(k.clone())
                            } else {
                                Anchor::Start
                            }
                        }
                    } else {
                        Anchor::Before(leaf.key_at(pos).expect("should exist").clone())
                    }
                }
                Cursor::After(pos) => {
                    if pos >= leaf.len {
                        if leaf.len > 0 {
                            Anchor::After(leaf.key_at(leaf.len - 1).expect("should exist").clone())
                        } else {
                            if let Some(k) = &leaf.upper_fence {
                                Anchor::After(k.clone())
                            } else {
                                Anchor::End
                            }
                        }
                    } else {
                        Anchor::After(leaf.key_at(pos).expect("should exist").clone())
                    }
                }
            };

            Some(anchor)
        } else {
            None
        }
    }

    fn restore_from_anchor(&mut self, anchor: &Anchor<K>) {
        self.parent.take();
        self.leaf.take(); // Make sure there are no locks held

        match anchor {
            Anchor::Start => {
                self.seek_to_first()
            }
            Anchor::End => {
                self.seek_to_last()
            }
            Anchor::Before(key) => {
                self.seek(key)
            }
            Anchor::After(key) => {
                self.seek_for_prev(key)
            }
        }
    }

    // Reimplementation of next_leaf and prev_leaf, previous implementation deadlocks (we cannot
    // try to lock a second leaf node without releasing the first)
    fn next_leaf(&mut self) -> LeafResult {
        if self.leaf.is_none() {
            return LeafResult::End;
        }

        let anchor = self.current_anchor().expect("leaf exists");

        loop {
            {
                let (guard, cursor) = self.leaf.as_ref().unwrap();
                let leaf = guard.as_leaf();
                match *cursor {
                    Cursor::Before(pos) if pos < leaf.len => {
                        // There is data on the leaf still
                        return LeafResult::Retry;
                    }
                    Cursor::After(pos) if (pos + 1) < leaf.len => {
                        // There is data on the leaf still
                        return LeafResult::Retry;
                    }
                    _ => {}
                }

                if leaf.upper_fence.is_none() || self.parent.is_none() {
                    // Leaf is root or there is no more data and no upper fence
                    return LeafResult::End;
                }
            }

            let _ = self.leaf.take(); // Drops leaf lock to try acquiring next (important)

            match self.optimistic_jump(Direction::Forward) {
                JumpResult::Ok => {
                    return LeafResult::Ok;
                }
                JumpResult::End => {
                    self.restore_from_anchor(&anchor);
                    continue;
                }
                JumpResult::Err(_) => {
                    self.restore_from_anchor(&anchor);
                    continue;
                }
            }
        }
    }

    fn prev_leaf(&mut self) -> LeafResult {
        if self.leaf.is_none() {
            return LeafResult::End;
        }

        let anchor = self.current_anchor().expect("leaf exists");

        loop {
            {
                let (guard, cursor) = self.leaf.as_ref().unwrap();
                let leaf = guard.as_leaf();
                match *cursor {
                    Cursor::Before(pos) if pos > 0 => {
                        // There is data on the leaf still
                        return LeafResult::Retry;
                    }
                    Cursor::After(_pos) => {
                        // There is data on the leaf still
                        return LeafResult::Retry;
                    }
                    _ => {}
                }

                if leaf.lower_fence.is_none() || self.parent.is_none() {
                    // Leaf is root or there is no more data and no lower fence
                    return LeafResult::End;
                }
            }

            let _ = self.leaf.take(); // Drops leaf lock to try acquiring next

            match self.optimistic_jump(Direction::Reverse) {
                JumpResult::Ok => {
                    return LeafResult::Ok;
                }
                JumpResult::End => {
                    self.restore_from_anchor(&anchor);
                    continue;
                }
                JumpResult::Err(_) => {
                    self.restore_from_anchor(&anchor);
                    continue;
                }
            }
        }
    }

    fn optimistic_jump(&mut self, direction: Direction) -> JumpResult {
        enum Outcome<L, P> {
            Leaf(L, u16),
            LeafAndParent(L, P, u16),
            End
        }

        let optmistic_perform = || {
            if let Some((parent_guard, p_cursor)) = self.parent.as_ref() {
                let bounded_pos = match direction {
                    Direction::Forward if *p_cursor < parent_guard.as_internal().len => Some(*p_cursor + 1),
                    Direction::Reverse if *p_cursor > 0 => Some(*p_cursor - 1),
                    _ => None
                };
                if let Some(pos) = bounded_pos {
                    let guard = {
                        let swip = parent_guard.as_internal().edge_at(pos)?;
                        GenericBPlusTree::lock_coupling(parent_guard, swip, &self.eg)?
                    };

                    assert!(guard.is_leaf());

                    error::Result::Ok(Outcome::Leaf(Self::leaf_lt(guard.to_exclusive()?), pos))
                } else {
                    let opt = match self.tree.find_nearest_leaf(parent_guard, direction, &self.eg)? { // TODO check if this is garanteed to return a sibling leaf
                        Some((guard, (parent, pos))) => {
                            Outcome::LeafAndParent(Self::leaf_lt(guard.to_exclusive()?), Self::parent_lt(parent), pos)
                        },
                        None => Outcome::End
                    };
                    error::Result::Ok(opt)
                }
            } else {
                error::Result::Ok(Outcome::End)
            }
        };


        match optmistic_perform() {
            Ok(Outcome::Leaf(leaf_guard, p_cursor)) => {
                let l_cursor = match direction {
                    Direction::Forward => Cursor::Before(0),
                    Direction::Reverse => Cursor::After(leaf_guard.as_leaf().len - 1)
                };

                self.leaf = Some((leaf_guard, l_cursor));
                self.parent.as_mut().map(|(_, p_c)| *p_c = p_cursor);

                return JumpResult::Ok;
            }
            Ok(Outcome::LeafAndParent(leaf_guard, parent_guard, p_cursor)) => {
                let l_cursor = match direction {
                    Direction::Forward => Cursor::Before(0),
                    Direction::Reverse => Cursor::After(leaf_guard.as_leaf().len - 1)
                };

                self.leaf = Some((leaf_guard, l_cursor));
                self.parent = Some((parent_guard, p_cursor));

                return JumpResult::Ok;
            }
            Ok(Outcome::End) => {
                return JumpResult::End;
            }
            Err(e) => {
                return JumpResult::Err(e);
            }
        }
    }

    pub fn seek<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().within_bounds(key) => {
                (guard, self.parent.take())
            }
            _ => {
                self.parent.take();
                let (guard, parent_opt) = self.tree.find_exclusive_leaf_and_optimistic_parent(key, &self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        let leaf = guard.as_leaf();
        let leaf_len = leaf.len;
        let (pos, _) = leaf.lower_bound(key);
        if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        }
    }

    pub fn seek_for_prev<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().within_bounds(key) => {
                (guard, self.parent.take())
            }
            _ => {
                self.parent.take();
                let (guard, parent_opt) = self.tree.find_exclusive_leaf_and_optimistic_parent(key, &self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        let leaf = guard.as_leaf();
        let leaf_len = leaf.len;
        let (pos, exact) = leaf.lower_bound(key);
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

    pub fn seek_exact<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().within_bounds(key) => {
                (guard, self.parent.take())
            }
            _ => {
                self.parent.take();
                let (guard, parent_opt) = self.tree.find_exclusive_leaf_and_optimistic_parent(key, &self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        let leaf = guard.as_leaf();
        let leaf_len = leaf.len;
        let (pos, exact) = leaf.lower_bound(key);
        if pos >= leaf_len {
            self.leaf = Some((guard, Cursor::Before(leaf_len)));
            self.parent = parent_opt;
        } else {
            self.leaf = Some((guard, Cursor::Before(pos)));
            self.parent = parent_opt;
        }

        return exact;
    }

    pub fn seek_to_first(&mut self) {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().lower_fence.is_none() => {
                (guard, self.parent.take())
            }
            _ => {
                self.parent.take();
                let (guard, parent_opt) = self.tree.find_first_exclusive_leaf_and_optimistic_parent(&self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        self.leaf = Some((guard, Cursor::Before(0)));
        self.parent = parent_opt;
    }

    pub fn seek_to_last(&mut self) {
        let (guard, parent_opt) = match self.leaf.take() {
            Some((guard, _)) if guard.as_leaf().upper_fence.is_none() => {
                (guard, self.parent.take())
            }
            _ => {
                self.parent.take();
                let (guard, parent_opt) = self.tree.find_last_exclusive_leaf_and_optimistic_parent(&self.eg);
                (Self::leaf_lt(guard), parent_opt.map(|(parent, pos)| (Self::parent_lt(parent), pos)))
            }
        };

        let leaf_len = guard.as_leaf().len;
        self.leaf = Some((guard, Cursor::Before(leaf_len)));
        self.parent = parent_opt;
    }

    pub fn insert(&mut self, key: K, value: V) {
        'start: loop {
            if self.seek_exact(&key) {
                let (_k, v) = self.next().unwrap();
                *v = value;
                break;
            } else {
                let (guard, cursor) = self.leaf.as_mut().expect("just seeked");
                if guard.as_leaf().has_space() {
                    let leaf = guard.as_leaf_mut();
                    match *cursor {
                        Cursor::Before(pos) => {
                            leaf.insert_at(pos, key, value).expect("just checked for space");
                        }
                        Cursor::After(_) => {
                            unreachable!("seek_exact always sets cursor to before");
                        }
                    }
                    break;
                } else {
                    self.parent.take();
                    let (guard, _cursor) = self.leaf.take().expect("just seeked");
                    let mut guard = guard.unlock();

                    loop {
                        let perform_split = || {
                            if !guard.as_leaf().has_space() {
                                guard.recheck()?;
                                self.tree.try_split(&guard, &self.eg)?;
                            }
                            error::Result::Ok(())
                        };

                        match perform_split() {
                            Ok(_) => break,
                            Err(error::Error::Reclaimed) => {
                                tp!("reclaimed");
                                continue 'start;
                            }
                            Err(_) => {
                                guard = guard.latch().optimistic_or_spin();
                                continue
                            }
                        }
                    }

                    // Split complete, try again
                    continue;
                }
            }
        }
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q> + Ord,
        Q: ?Sized + Ord
    {
        if self.seek_exact(key) {
            Some(self.remove_next().expect("just seeked for remove"))
        } else {
            None
        }
    }

    fn remove_next(&mut self) -> Option<(K, V)> {
        match self.leaf.as_mut() {
            Some((guard, cursor)) => {
                let leaf = guard.as_leaf_mut();

                let removed = match cursor {
                    Cursor::Before(pos) => {
                        let curr_pos = *pos;
                        if curr_pos < leaf.len {
                            Some(leaf.remove_at(curr_pos))
                        } else {
                            None
                        }
                    }
                    Cursor::After(pos) => {
                        let pos = *pos;
                        let curr_pos = pos + 1;
                        if curr_pos < leaf.len {
                            Some(leaf.remove_at(curr_pos))
                        } else {
                            None
                        }
                    }
                };

                if let Some((removed_key, _)) = removed.as_ref() {
                    if guard.is_underfull() {
                        self.parent.take();
                        let (guard, _cursor) = self.leaf.take().expect("just seeked");

                        let guard = guard.unlock();
                        loop {
                            let perform_merge = || {
                                let _ = self.tree.try_merge(&guard, &self.eg)?;
                                error::Result::Ok(())
                            };

                            match perform_merge() {
                                Ok(_) => {
                                    break;
                                },
                                Err(error::Error::Reclaimed) => {
                                    break;
                                }
                                Err(_) => {
                                    break; // TODO not ensuring merges
                                    // guard = guard.latch().optimistic_or_spin();
                                    // continue
                                }
                            }
                        }

                        self.seek(removed_key);
                    }
                }

                removed
            }
            None => {
                None
            }
        }
    }

    #[inline]
    pub fn next(&mut self) -> Option<(&K, &mut V)> {
        loop {
            let opt = match self.leaf.as_ref() {
                Some((guard, cursor)) => {
                    let leaf = guard.as_leaf();
                    match cursor {
                        Cursor::Before(pos) => {
                            let pos = *pos;
                            if pos < leaf.len {
                                Some((pos, Cursor::Before(pos + 1)))
                            } else {
                                None
                            }
                        },
                        Cursor::After(pos) => {
                            let pos = *pos;
                            let curr_pos = pos + 1;
                            if curr_pos < leaf.len {
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
                return Some(leaf.kv_at_mut(curr_pos).expect("should exist"));
            } else {
                match self.next_leaf() {
                    LeafResult::Ok => {
                        // Try to fetch next from this new leaf
                        continue;
                    }
                    LeafResult::Retry => {
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
    pub fn prev(&mut self) -> Option<(&K, &mut V)> {
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
                return Some(leaf.kv_at_mut(curr_pos).expect("should exist"));
            } else {
                match self.prev_leaf() {
                    LeafResult::Ok => {
                        // Try to fetch prev from this new leaf
                        continue;
                    }
                    LeafResult::Retry => {
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

#[cfg(test)]
mod tests {
    use crate::{BPlusTree};
    use crossbeam_epoch::{self as epoch, Atomic};
    use crate::util::sample_tree;
    use super::{RawSharedIter, RawExclusiveIter};
    use serial_test::serial;

    macro_rules! kv {
        ($n:expr) => {
            Some((&format!("{:04}", $n), &$n))
        };
    }

    macro_rules! kv_mut {
        ($n:expr) => {
            Some((&format!("{:04}", $n), &mut $n))
        };
    }

    #[test]
    fn shared_iter_works() {
        let bptree = sample_tree("fixtures/sample.json");

        let mut iter = RawSharedIter {
            tree: &bptree,
            eg: epoch::pin(),
            parent: None,
            leaf: None
        };

        iter.seek("0001");
        assert_eq!(iter.next(), kv!(2));
        assert_eq!(iter.next(), kv!(3));
        assert_eq!(iter.next(), kv!(5));
        assert_eq!(iter.next(), None);

        iter.seek_for_prev("0005");

        assert_eq!(iter.prev(), kv!(5));
        assert_eq!(iter.next(), kv!(5));

        assert_eq!(iter.prev(), kv!(5));
        assert_eq!(iter.prev(), kv!(3));
        assert_eq!(iter.prev(), kv!(2));
        assert_eq!(iter.prev(), None);

        assert_eq!(iter.seek_exact("0001"), false);
        assert_eq!(iter.next(), kv!(2));

        assert_eq!(iter.seek_exact("0003"), true);
        assert_eq!(iter.next(), kv!(3));
    }

    #[test]
    fn exclusive_iter_works() {
        let bptree = sample_tree("fixtures/sample.json");

        let mut iter = RawExclusiveIter {
            tree: &bptree,
            eg: epoch::pin(),
            parent: None,
            leaf: None
        };

        iter.seek("0001");
        assert_eq!(iter.next(), kv_mut!(2));
        assert_eq!(iter.next(), kv_mut!(3));
        assert_eq!(iter.next(), kv_mut!(5));
        assert_eq!(iter.next(), None);

        iter.seek_for_prev("0005");

        assert_eq!(iter.prev(), kv_mut!(5));
        assert_eq!(iter.next(), kv_mut!(5));

        assert_eq!(iter.prev(), kv_mut!(5));
        assert_eq!(iter.prev(), kv_mut!(3));
        assert_eq!(iter.prev(), kv_mut!(2));
        assert_eq!(iter.prev(), None);

        assert_eq!(iter.seek_exact("0001"), false);
        assert_eq!(iter.next(), kv_mut!(2));

        assert_eq!(iter.seek_exact("0003"), true);
        assert_eq!(iter.next(), kv_mut!(3));

        {
            let (k, v) = iter.next().unwrap();
            *v = 6;
        }

        assert_eq!(iter.next(), None);
        assert_eq!(iter.prev(), Some((&"0005".to_string(), &mut 6)));
    }

    #[test]
    fn exclusive_iter_insert() {
        let bptree = sample_tree("fixtures/sample.json");

        let mut iter = RawExclusiveIter {
            tree: &bptree,
            eg: epoch::pin(),
            parent: None,
            leaf: None
        };

        iter.seek("0001");
        assert_eq!(iter.next(), kv_mut!(2));
        assert_eq!(iter.next(), kv_mut!(3));
        assert_eq!(iter.next(), kv_mut!(5));
        assert_eq!(iter.next(), None);

        iter.insert("0001".to_string(), 1);

        iter.seek("");
        assert_eq!(iter.next(), kv_mut!(1));
        assert_eq!(iter.next(), kv_mut!(2));
        assert_eq!(iter.next(), kv_mut!(3));
        assert_eq!(iter.next(), kv_mut!(5));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn exclusive_iter_insert_empty() {
        let bptree: BPlusTree<String, u64> = BPlusTree::new();

        let mut iter = RawExclusiveIter {
            tree: &bptree,
            eg: epoch::pin(),
            parent: None,
            leaf: None
        };

        for i in 0..10000 {
            iter.insert(format!("{:06}", i), i);
        }

        iter.seek("");

        for mut i in 0..10000 {
            assert_eq!(iter.next(), Some((&format!("{:06}", i), &mut i)));
        }

        assert_eq!(iter.next(), None);

        for mut i in (0..10000).rev() {
            assert_eq!(iter.prev(), Some((&format!("{:06}", i), &mut i)));
        }

        let found = bptree.lookup("009999", |value| *value); // Caution, using this while holding the iterator may deadlock;
        assert_eq!(found, Some(9999));
    }

    #[test]
    #[serial]
    fn exclusive_iter_remove() {
        use rand::thread_rng;
        use rand::seq::SliceRandom;

        let bptree: BPlusTree<String, usize> = BPlusTree::new();

        let mut iter = RawExclusiveIter {
            tree: &bptree,
            eg: epoch::pin(),
            parent: None,
            leaf: None
        };

        let n = 1000000usize;

        let mut data: Vec<_> = (0..n).collect();

        let t0 = std::time::Instant::now();
        data.shuffle(&mut thread_rng());
        println!("took: {:?}", t0.elapsed());

        let mut strings = vec!();

        let t0 = std::time::Instant::now();
        for i in 0..n {
            strings.push(format!("{:09}", data[i]));
        }
        println!("took: {:?}", t0.elapsed());

        let mut sorted_data = data.clone();
        sorted_data.sort();
        let mut sorted_strings = strings.clone();
        sorted_strings.sort();


        println!("BPlusTree");

        let t0 = std::time::Instant::now();
        for i in 0..n {
            iter.insert(strings[i].clone(), data[i]);
        }
        println!("took: {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        iter.seek("");
        println!("took: {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        for mut i in 0..n {
            assert_eq!(iter.next(), Some((&sorted_strings[i], &mut sorted_data[i])));
        }

        assert_eq!(iter.next(), None);

        println!("took: {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        for mut i in (0..n).rev() {
            assert_eq!(iter.remove(&strings[i]).as_ref().map(|(k, v)| (k, v)), Some((&strings[i], &data[i])));
        }
        println!("took: {:?}", t0.elapsed());

        // let found = bptree.lookup("009999", |value| *value); // Caution, using this while holding the iterator may deadlock;
        // assert_eq!(found, None);

        let t0 = std::time::Instant::now();
        let mut count = 0;
        iter.seek_to_first();
        while let Some(_) = iter.next() {
            count += 1;
        }
        println!("took: {:?}", t0.elapsed());

        assert_eq!(count, 0);

        println!("BTreeMap");

        use std::collections::BTreeMap;

        let mut btreemap: BTreeMap<String, usize> = BTreeMap::default();

        let t0 = std::time::Instant::now();
        for i in 0..n {
            btreemap.insert(strings[i].clone(), data[i]);
        }
        println!("took: {:?}", t0.elapsed());

        let t0 = std::time::Instant::now();
        btreemap.contains_key("");
        println!("took: {:?}", t0.elapsed());

        {
            let mut iter = btreemap.iter_mut();
            let t0 = std::time::Instant::now();
            for mut i in 0..n {
                assert_eq!(iter.next(), Some((&sorted_strings[i], &mut sorted_data[i])));
            }

            assert_eq!(iter.next(), None);

            println!("took: {:?}", t0.elapsed());
        }

        let t0 = std::time::Instant::now();
        for mut i in (0..n).rev() {
            assert_eq!(btreemap.remove(&strings[i]).as_ref(), Some(&data[i]));
        }
        println!("took: {:?}", t0.elapsed());

        {
            let t0 = std::time::Instant::now();
            let mut count = 0;
            let mut iter = btreemap.iter();
            while let Some(_) = iter.next() {
                count += 1;
            }
            println!("took: {:?}", t0.elapsed());

            assert_eq!(count, 0);
        }
    }
}
