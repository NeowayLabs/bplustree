use std::fmt;
use std::slice;

use crate::dbg_global_report;
use crate::persistent::bufmgr::{
    swip::{Swip, Pid},
    BufferFrame
};

use crate::{
    error::{self, NonOptimisticExt},
    latch::HybridLatch
};

use super::bufmgr::BufferManager;


#[derive(Default, PartialEq, Hash, Copy, Clone)]
#[repr(C)]
pub(crate) struct Data; // TODO private

#[derive(Default, PartialEq, Hash, Copy, Clone)]
#[repr(C)]
struct Fence {
    offset: usize,
    len: usize
}

struct BoxedNode {
    storage: Box<[u8]>
}

impl BoxedNode {
    fn new(is_leaf: bool, capacity: usize) -> Self {
        let storage_size = std::mem::size_of::<Node>() + capacity;
        let mut storage = vec![0u8; storage_size].into_boxed_slice();
        let node = unsafe { &mut *(storage.as_mut_ptr() as *mut Node) };
        node.init(is_leaf, capacity);

        BoxedNode {
            storage
        }
    }

    fn copy_to(&self, other: &mut Node) {
        let storage_len = self.storage.len();
        unsafe { std::ptr::copy_nonoverlapping(self.storage.as_ptr(), other as *mut Node as *mut u8, storage_len) };
    }
}

impl std::ops::Deref for BoxedNode {
    type Target = Node;

    #[inline]
    fn deref(&self) -> &Node {
        unsafe { &*(self.storage.as_ptr() as *const Node) }
    }
}

impl std::ops::DerefMut for BoxedNode {
    #[inline]
    fn deref_mut(&mut self) -> &mut Node {
        unsafe { &mut *(self.storage.as_mut_ptr() as *mut Node) }
    }
}

const HINT_COUNT: usize = 16;

fn to_u64(slice: &[u8]) -> u64 {
    use std::convert::TryInto;
    u64::from_be_bytes(slice.try_into().unwrap()) // FIXME debug
}

pub enum NodeKind<L, I> {
    Leaf(L),
    Internal(I)
}

#[repr(C)]
pub struct Node {
    capacity: usize,
    is_leaf: bool,
    len: usize,
    space_used: usize,
    data_offset: usize,
    prefix_len: usize,
    lower_fence: Fence,
    upper_fence: Fence,
    upper_edge: Option<Swip<HybridLatch<BufferFrame>>>,
    hints: [u32; HINT_COUNT],
    pub(crate) data: Data // Data starts at `addr_of(self.data)` and ends at `addr_of(self.data) + self.capacity` // TODO private
}

impl Node {
    pub(crate) fn init(&mut self, is_leaf: bool, capacity: usize) {
        self.capacity = capacity;
        self.is_leaf = is_leaf;
        self.len = 0;
        self.space_used = 0;
        self.data_offset = capacity;
        self.prefix_len = 0;
        self.lower_fence = Fence::default();
        self.upper_fence = Fence::default();
        self.upper_edge = None;
        self.hints = [0u32; HINT_COUNT];
        // TODO zero out data?
    }

    pub(crate) fn hints(&self) -> &[u32] {
        &self.hints
    }

    #[inline]
    pub fn as_leaf(&self) -> &LeafNode {
        if self.is_leaf {
            unsafe { std::mem::transmute::<_, &LeafNode>(self) }
        } else {
            panic!("not a leaf node");
        }
    }

    #[inline]
    pub fn as_internal(&self) -> &InternalNode {
        if !self.is_leaf {
            unsafe { std::mem::transmute::<_, &InternalNode>(self) }
        } else {
            panic!("not an internal node");
        }
    }

    #[inline]
    pub(crate) fn as_leaf_mut(&mut self) -> &mut LeafNode {
        if self.is_leaf {
            unsafe { std::mem::transmute::<_, &mut LeafNode>(self) }
        } else {
            panic!("not a leaf node");
        }
    }

    #[inline]
    pub(crate) fn as_internal_mut(&mut self) -> &mut InternalNode {
        if !self.is_leaf {
            unsafe { std::mem::transmute::<_, &mut InternalNode>(self) }
        } else {
            panic!("not an internal node");
        }
    }

    #[inline]
    pub(crate) fn try_leaf(&self) -> error::Result<&LeafNode> {
        if self.is_leaf {
            Ok(unsafe { std::mem::transmute::<_, &LeafNode>(self) })
        } else {
            Err(error::Error::Unwind)
        }
    }

    #[inline]
    pub(crate) fn try_internal(&self) -> error::Result<&InternalNode> {
        if !self.is_leaf {
            Ok(unsafe { std::mem::transmute::<_, &InternalNode>(self) })
        } else {
            Err(error::Error::Unwind)
        }
    }

    #[inline]
    pub(crate) fn downcast<'a>(&'a self) -> NodeKind<&'a LeafNode, &'a InternalNode> {
        if self.is_leaf {
            NodeKind::Leaf(unsafe { std::mem::transmute::<_, &'a LeafNode>(self) })
        } else {
            NodeKind::Internal(unsafe { std::mem::transmute::<_, &'a InternalNode>(self) })
        }
    }

    #[inline]
    pub(crate) fn downcast_mut<'a>(&'a mut self) -> NodeKind<&'a mut LeafNode, &'a mut InternalNode> {
        if self.is_leaf {
            NodeKind::Leaf(unsafe { std::mem::transmute::<_, &'a mut LeafNode>(self) })
        } else {
            NodeKind::Internal(unsafe { std::mem::transmute::<_, &'a mut InternalNode>(self) })
        }
    }

    #[inline]
    pub(crate) fn is_underfull(&self) -> bool {
        match self.downcast() {
            NodeKind::Leaf(l) => l.is_underfull(),
            NodeKind::Internal(i) => i.is_underfull()
        }
    }

    #[inline]
    fn calculate_prefix_len(
        lower_fence_key: Option<&[u8]>,
        upper_fence_key: Option<&[u8]>) -> usize
    {
        let lower_key = lower_fence_key.unwrap_or(&[]);
        let upper_key = upper_fence_key.unwrap_or(&[]);

        let mut prefix_len = 0;
        while (prefix_len < lower_key.len().min(upper_key.len())) && (lower_key[prefix_len] == upper_key[prefix_len])
        {
            prefix_len += 1;
        }

        prefix_len
    }

    #[inline]
    pub(crate) fn try_can_merge_with(&self, right_node: &Self) -> error::Result<bool> {
        match self.downcast() {
            NodeKind::Leaf(left) => {
                let right = right_node.try_leaf()?;
                Ok(left.can_merge_with(right))
            },
            NodeKind::Internal(left) => {
                let right = right_node.try_internal()?;
                Ok(left.can_merge_with(right))
            }
        }
    }

    #[inline]
    pub(crate) fn is_leaf(&self) -> bool {
        self.is_leaf
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn lower_fence(&self) -> error::Result<Option<&[u8]>> {
        if self.lower_fence == Fence::default() {
            return Ok(None)
        }

        Ok(Some(self.sized(self.lower_fence.offset, self.lower_fence.len)?))
    }

    #[inline]
    pub(crate) fn upper_fence(&self) -> error::Result<Option<&[u8]>> {
        if self.upper_fence == Fence::default() {
            return Ok(None)
        }

        Ok(Some(self.sized(self.upper_fence.offset, self.upper_fence.len)?))
    }

    #[inline]
    pub(crate) fn prefix(&self) -> error::Result<&[u8]> {
        if let Some(fence) = self.lower_fence()? {
            Ok(unsafe { fence.get_unchecked(..self.prefix_len) })
        } else {
            Ok(self.sized(0, 0)?)
        }
    }

    #[inline]
    fn search_hint(&self, head: u32) -> (usize, usize) {
        let mut pos1 = 0;

        while pos1 < HINT_COUNT {
            if unsafe { *self.hints.get_unchecked(pos1) } >= head {
                break;
            }
            pos1 += 1;
        }

        let mut pos2 = pos1;

        while pos2 < HINT_COUNT {
            if unsafe { *self.hints.get_unchecked(pos2) } != head {
                break;
            }
            pos2 += 1;
        }

        (pos1, pos2)
    }

    #[inline]
    fn sized(&self, offset: usize, len: usize) -> error::Result<&[u8]> {
        let start = offset;
        // TODO we should have a capacity mask here
        let end = start + len;// (start + len).min(self.capacity); // start.saturating_add(len);
        // let end = (start + len) & 0b0000000000000000000000000000000000000000000000001111111111111111;// (start + len).min(self.capacity);

        Ok(self.range(start, end)?)
    }

    #[inline]
    fn sized_mut(&mut self, offset: usize, len: usize) -> &mut [u8] {
        let start = offset;
        let end = start + len;// (start + len).min(self.capacity);

        self.range_mut(start, end)
    }

    #[inline]
    fn range(&self, start: usize, end: usize) -> error::Result<&[u8]> {
        // debug_assert!(end <= self.capacity);
        if end > self.capacity {
            return Err(error::Error::Unwind);
        }

        Ok(unsafe { self.data().get_unchecked(start..end) })
    }

    #[inline]
    fn range_mut(&mut self, start: usize, end: usize) -> &mut [u8] {
        debug_assert!(end <= self.capacity);

        unsafe { self.data_mut().get_unchecked_mut(start..end) }
    }

    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(std::ptr::addr_of!(self.data) as *const u8, self.capacity) }
    }

    #[inline]
    fn data_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(std::ptr::addr_of_mut!(self.data) as *mut u8, self.capacity) }
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("capacity", &self.capacity)
            .field("is_leaf", &self.is_leaf)
            .field("len", &self.len)
            .field("space_used", &self.space_used)
            .field("data_offset", &self.data_offset)
            .field("lower_fence", &self.lower_fence())
            .field("upper_fence", &self.upper_fence())
            .field("data", &self.data())
            .finish()
    }
}

fn to_head(slice: &[u8]) -> u32 {
    match slice.len() {
        0 => 0,
        1 => (unsafe { *slice.get_unchecked(0) } as u32) << 24,
        2 => (
            u16::from_ne_bytes([
                unsafe{ *slice.get_unchecked(1) },
                unsafe{ *slice.get_unchecked(0) }
            ]) as u32
        ) << 16,
        3 => (
                u16::from_ne_bytes([
                    unsafe{ *slice.get_unchecked(1) },
                    unsafe{ *slice.get_unchecked(0) }
                ]) as u32
            ) << 16 |
            (
                unsafe { *slice.get_unchecked(2) } as u32
            ) << 8,
        _ => u32::from_ne_bytes([
            unsafe{ *slice.get_unchecked(3) },
            unsafe{ *slice.get_unchecked(2) },
            unsafe{ *slice.get_unchecked(1) },
            unsafe{ *slice.get_unchecked(0) }
        ])
    }
}

#[derive(Debug, Default, Hash, PartialEq, Copy, Clone)]
pub(crate) struct SplitEntryHint<'a> {
    pub(crate) pos: usize,
    pub(crate) key: &'a [u8],
    pub(crate) value_len: usize
}

#[derive(Debug, Hash, PartialEq, Clone)]
pub(crate) enum SplitStrategy {
    SplitAt {
        key: Vec<u8>,
        split_pos: usize,
        new_left_size: Option<usize>,
        right_size: usize,
    },
    Grow {
        new_size: usize,
    }
}

impl SplitStrategy {
    pub(crate) fn split_key(&self) -> &[u8] {
        match self {
            SplitStrategy::SplitAt { key, .. } => key.as_slice(),
            SplitStrategy::Grow { .. } => &[],
        }
    }
}



#[derive(Debug, Hash, PartialEq, Clone)]
pub(crate) enum SplitMeta {
    AtPos {
        key: Vec<u8>,
        at: usize,
    },
    BeforePos {
        key: Vec<u8>,
        before: usize,
    }
}

impl SplitMeta {
    pub(crate) fn split_key(&self) -> &[u8] {
        match self {
            SplitMeta::AtPos { key, .. } => key.as_slice(),
            SplitMeta::BeforePos { key, .. } => key.as_slice(),
        }
    }
    pub(crate) fn into_split_key(self) -> Vec<u8> {
        match self {
            SplitMeta::AtPos { key, .. } => key,
            SplitMeta::BeforePos { key, .. } => key,
        }
    }
    pub(crate) fn into_parts(self) -> (Vec<u8>, usize, bool) {
        match self {
            SplitMeta::AtPos { key, at } => (key, at, true),
            SplitMeta::BeforePos { key, before } => (key, before, false),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Copy, Clone)]
pub(crate) enum BoundaryOrdering {
    Before,
    Within,
    After
}

#[derive(Debug, Default, PartialEq, Hash, Copy, Clone)]
#[repr(C)]
struct Slot {
    offset: usize,
    key_len: u32,
    head: u32,
    payload_len: usize
}

#[repr(C)]
pub struct LeafNode {
    pub(crate) base: Node,
    data: ()
}

impl LeafNode {
    fn free_space(&self) -> usize {
        self.base.capacity - (self.base.capacity - self.base.data_offset) - (std::mem::size_of::<Slot>() * self.base.len)
    }

    fn free_space_after_compaction(&self) -> usize {
        self.base.capacity - self.base.space_used - (std::mem::size_of::<Slot>() * self.base.len)
    }

    fn used_capacity_after_compaction(&self) -> usize {
        self.base.space_used + (std::mem::size_of::<Slot>() * self.base.len)
    }

    fn has_enough_space_for(&self, space_needed: usize) -> bool {
        space_needed <= self.free_space() || space_needed <= self.free_space_after_compaction()
    }

    fn request_space_for(&mut self, space_needed: usize) -> bool {
        if space_needed <= self.free_space() {
            true
        } else if space_needed <= self.free_space_after_compaction() {
            self.compactify();
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn is_underfull(&self) -> bool {
        self.free_space_after_compaction() < (self.base.capacity as f32 * 0.4) as usize
    }

    fn slots(&self) -> &[Slot] {
        unsafe { slice::from_raw_parts(std::ptr::addr_of!(self.base.data) as *const Slot, self.base.len) }
    }

    fn slots_mut(&mut self) -> &mut [Slot] {
        unsafe { slice::from_raw_parts_mut(std::ptr::addr_of_mut!(self.base.data) as *mut Slot, self.base.len) }
    }

    fn slots_with_len_mut(&mut self, new_len: usize) -> &mut [Slot] {
        unsafe { slice::from_raw_parts_mut(std::ptr::addr_of_mut!(self.base.data) as *mut Slot, new_len) }
    }

    fn slots_from_data_mut(data: &mut Data, new_len: usize) -> &mut [Slot] {
        unsafe { slice::from_raw_parts_mut(data as *mut Data as *mut Slot, new_len) }
    }

    #[inline]
    pub(crate) fn key_at(&self, pos: usize) -> error::Result<&[u8]> {
        let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
        Ok(self.base.sized(slot.offset, slot.key_len as usize)?)
    }

    #[inline]
    fn full_key_len(&self, pos: usize) -> error::Result<usize> {
        let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
        Ok(self.base.prefix_len + slot.key_len as usize)
    }

    #[inline]
    pub(crate) fn full_key_at(&self, pos: usize) -> error::Result<Vec<u8>> {
        let prefix = self.base.prefix()?;
        let suffix = self.key_at(pos)?;
        let mut out = Vec::with_capacity(prefix.len() + suffix.len());
        out.extend_from_slice(prefix);
        out.extend_from_slice(suffix);
        Ok(out)
    }

    #[inline]
    pub(crate) fn copy_full_key_at(&self, pos: usize, buffer: &mut Vec<u8>) -> error::Result<()> {
        let prefix = self.base.prefix()?;
        let suffix = self.key_at(pos)?;
        buffer.truncate(0);
        buffer.extend_from_slice(prefix);
        buffer.extend_from_slice(suffix);
        Ok(())
    }

    #[inline]
    pub(crate) fn value_at(&self, pos: usize) -> error::Result<&[u8]> {
        let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
        Ok(self.base.sized(slot.offset + slot.key_len as usize, slot.payload_len)?)
    }

    #[inline]
    pub(crate) fn value_at_mut(&mut self, pos: usize) -> &mut [u8] {
        let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
        let offset = slot.offset;
        let key_len = slot.key_len;
        let payload_len = slot.payload_len;
        self.base.sized_mut(offset + key_len as usize, payload_len)
    }

    #[inline]
    fn kv_len(&self, pos: usize) -> error::Result<usize> {
        let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
        Ok(slot.key_len as usize + slot.payload_len)
    }

    #[inline]
    fn kv(&self, pos: usize) -> error::Result<&[u8]> {
        let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
        Ok(self.base.sized(slot.offset, slot.key_len as usize + slot.payload_len)?)
    }

    #[inline]
    pub(crate) fn lower_bound_exact<K: AsRef<[u8]>>(&self, key: K) -> error::Result<Option<usize>> {
        let key = key.as_ref();
        if (key.len() < self.base.prefix_len) || &key[..self.base.prefix_len] != self.base.prefix()? {
            return Ok(None);
        }

        let (pos, exact) = self.lower_bound_suffix(unsafe { key.get_unchecked(self.base.prefix_len..) })?;

        if exact {
            Ok(Some(pos))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn lower_bound<K: AsRef<[u8]>>(&self, key: K) -> error::Result<(usize, bool)> {
        use std::cmp::Ordering;

//          let first_hint = self.base.hints[0];
//          if self.base.hints.iter().all(|&h| h == first_hint) {
//              return self.lower_bound_bench(key);
//          }

        let key = key.as_ref();
        let cmp_len = key.len().min(self.base.prefix_len);
        match unsafe { key.get_unchecked(..cmp_len).cmp(self.base.prefix()?) } {
            Ordering::Less => {
                return Ok((0, false));
            }
            Ordering::Greater => {
                return Ok((self.base.len, false));
            }
            Ordering::Equal => {}
        }

        self.lower_bound_suffix(unsafe { key.get_unchecked(self.base.prefix_len..) })
    }

    #[inline]
    pub fn lower_bound_bench<K: AsRef<[u8]>>(&self, key: K) -> error::Result<(usize, bool)> {
        use std::cmp::Ordering;

        let key = key.as_ref();
        let cmp_len = key.len().min(self.base.prefix_len);
        match unsafe { key.get_unchecked(..cmp_len).cmp(self.base.prefix()?) } {
            Ordering::Less => {
                return Ok((0, false));
            }
            Ordering::Greater => {
                return Ok((self.base.len, false));
            }
            Ordering::Equal => {}
        }

        let suffix = unsafe { key.get_unchecked(self.base.prefix_len..) };

        let mut lower = 0;
        let mut upper = self.base.len;

        while lower < upper {
            let mid = ((upper - lower) / 2) + lower;

            let mid_key = self.key_at(mid)?;
            if suffix < mid_key {
                upper = mid;
            } else if suffix > mid_key {
                lower = mid + 1;
            } else {
                return Ok((mid, true));
            }
        }

        Ok((lower, false))
    }

    #[inline]
    fn lower_bound_suffix<K: AsRef<[u8]>>(&self, key: K) -> error::Result<(usize, bool)> {
        let key = key.as_ref();
        let mut lower = 0;
        let mut upper = self.base.len;
        let head = to_head(key);

        if self.base.len > HINT_COUNT * 2 {
            let dist = self.base.len / (HINT_COUNT + 1);
            let (pos1, pos2) = self.base.search_hint(head);
            lower = pos1 * dist;
            if pos2 < HINT_COUNT {
                upper = (pos2 + 1) * dist;
            }
        }

        let slots = self.slots();

        while lower < upper {
            let mid = ((upper - lower) / 2) + lower;

            let mid_slot = unsafe { slots.get_unchecked(mid) };

            if head < mid_slot.head {
                upper = mid;
            } else if head > mid_slot.head {
                lower = mid + 1;
            } else if mid_slot.key_len <= 4 {
                // head is equal, we don't have to check the rest of the key
                if key.len() < mid_slot.key_len as usize {
                    upper = mid;
                } else if key.len() > mid_slot.key_len as usize {
                    lower = mid + 1;
                } else {
                    return Ok((mid, true));
                }
            } else {
                let mid_key = self.key_at(mid)?;
                if key < mid_key {
                    upper = mid;
                } else if key > mid_key {
                    lower = mid + 1;
                } else {
                    return Ok((mid, true));
                }
            }
        }

        Ok((lower, false))
    }

    fn make_hint(&mut self) {
        let dist = self.base.len / (HINT_COUNT + 1);
        for i in 0..HINT_COUNT {
            unsafe {
                *self.base.hints.get_unchecked_mut(i) = self.slots().get_unchecked(dist * (i + 1)).head;
            }
        }
    }

    fn update_hint(&mut self, pos: usize) {
        let dist = self.base.len / (HINT_COUNT + 1);
        let mut start = 0;
        if (self.base.len > (HINT_COUNT * 2 + 1))
            && ((self.base.len - 1) / (HINT_COUNT + 1) == dist)
            && (pos / dist > 1)
        {
            start = (pos / dist) - 1;
        }

        for i in start..HINT_COUNT {
            unsafe {
                *self.base.hints.get_unchecked_mut(i) = self.slots().get_unchecked(dist * (i + 1)).head;
            }
        }

        // TODO remove check?
//          for i in 0..HINT_COUNT {
//              unsafe {
//                  assert_eq!(*self.base.hints.get_unchecked(i), self.slots().get_unchecked(dist * (i + 1)).head);
//              }
//          }
    }

    fn space_needed_with_prefix_len(&self, key_len: usize, payload_len: usize, prefix_len: usize) -> usize {
        std::mem::size_of::<Slot>() + (key_len - prefix_len) + payload_len
    }

    fn space_needed(&self, key_len: usize, payload_len: usize) -> usize {
        self.space_needed_with_prefix_len(key_len, payload_len, self.base.prefix_len)
    }

    pub(crate) fn can_insert(&self, key_len: usize, payload_len: usize) -> bool {
        let space_needed = self.space_needed(key_len, payload_len);
        self.has_enough_space_for(space_needed)
    }

    fn prepare_insert(&mut self, key_len: usize, payload_len: usize) -> bool {
        let space_needed = self.space_needed(key_len, payload_len);
        if !self.request_space_for(space_needed) {
            false
        } else {
            true
        }
    }

    // TODO insert_reserve_payload

    pub(crate) fn insert<K: AsRef<[u8]>, P: AsRef<[u8]>>(&mut self, key: K, payload: P) -> Option<usize> {
        debug_assert!(self.can_insert(key.as_ref().len(), payload.as_ref().len()));
        if !self.prepare_insert(key.as_ref().len(), payload.as_ref().len()) {
            return None;
        }

        let (pos, _) = self.lower_bound(key.as_ref()).unopt();
        let curr_len = self.base.len;
        let new_slots = self.slots_with_len_mut(self.base.len + 1);
        new_slots.copy_within(pos..curr_len, pos + 1);
        self.store_key_value(pos, key, payload); // TODO This may write past slots len, consider changing len before this
        self.base.len += 1;
        self.update_hint(pos);
        Some(pos)
    }

    pub(crate) fn insert_at<K: AsRef<[u8]>, P: AsRef<[u8]>>(&mut self, pos: usize, key: K, payload: P) -> Option<usize> {
        debug_assert!(self.can_insert(key.as_ref().len(), payload.as_ref().len()));
        if !self.prepare_insert(key.as_ref().len(), payload.as_ref().len()) {
            return None;
        }

        let curr_len = self.base.len;
        let new_slots = self.slots_with_len_mut(self.base.len + 1);
        new_slots.copy_within(pos..curr_len, pos + 1);
        self.store_key_value(pos, key, payload); // TODO This may write past slots len, consider changing len before this
        self.base.len += 1;
        self.update_hint(pos);
        Some(pos)
    }

    fn set_lower_fence<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        let key_len = key.len();
//         if key_len < 9 && key_len > 0 {
//             println!("lower fence key {:?}", key);
//             panic!("test");
//         }
        assert!(self.free_space() >= key_len);
        self.base.data_offset -= key_len;
        self.base.space_used += key_len;
        self.base.lower_fence = Fence {
            offset: self.base.data_offset,
            len: key_len
        };
        self.base.sized_mut(self.base.data_offset, key_len).copy_from_slice(key);
    }

    fn set_upper_fence<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        let key_len = key.len();
        assert!(self.free_space() >= key_len);
        self.base.data_offset -= key_len;
        self.base.space_used += key_len;
        self.base.upper_fence = Fence {
            offset: self.base.data_offset,
            len: key_len
        };
        self.base.sized_mut(self.base.data_offset, key_len).copy_from_slice(key);
    }

    fn set_fences(&mut self, lower_fence_key: Option<&[u8]>, upper_fence_key: Option<&[u8]>) {
        let lower_key = if let Some(lower_fence_key) = lower_fence_key {
            self.set_lower_fence(lower_fence_key);
            lower_fence_key
        } else {
            &[]
        };

        let upper_key = if let Some(upper_fence_key) = upper_fence_key {
            self.set_upper_fence(upper_fence_key);
            upper_fence_key
        } else {
            &[]
        };

        let mut prefix_len = 0;
        while (prefix_len < lower_key.len().min(upper_key.len())) && (lower_key[prefix_len] == upper_key[prefix_len])
        {
            prefix_len += 1;
        }

        self.base.prefix_len = prefix_len;
    }

    fn store_key_value<K: AsRef<[u8]>, P: AsRef<[u8]>>(&mut self, pos: usize, key: K, payload: P) {
        use std::convert::TryInto;

        let key = key.as_ref();
        let payload = payload.as_ref();

        #[cfg(debug_assertions)]
        if key.len() == 8 && key.len() == payload.len() && key != payload {
            let k = to_u64(key);
            let v = to_u64(payload);
            crate::dbg_global_report!();
            crate::dbg_local_report!();
            println!("wrong kv, key = {}, value = {}", k, v);
            panic!("boom");
        }

        #[cfg(debug_assertions)]
        if &key[..self.base.prefix_len] != self.base.prefix().unopt() {
            let lower = to_u64(self.base.lower_fence().unopt().unwrap_or(&[0, 0, 0, 0, 0, 0, 0, 0]));
            let upper = to_u64(self.base.upper_fence().unopt().unwrap_or(&[255, 255, 255, 255, 255, 255, 255, 255]));
            let k = to_u64(key);
            println!("bug upper: {}, lower: {}, k: {}", upper, lower, k);
            println!("Custom backtrace: {}", std::backtrace::Backtrace::force_capture());
            crate::dbg_local_report!();
            crate::dbg_global_report!();

            std::process::exit(3);
        }

        let key_suffix = &key[self.base.prefix_len..];

        let key_len = key_suffix.len();
        let payload_len = payload.len();


        let head = to_head(key_suffix);
        let full_size = key_len + payload_len;
        // println!("{}, {},  {}, {}", self.base.capacity, self.base.data_offset, key_len, payload_len);
        self.base.data_offset -= full_size;
        self.base.space_used += full_size;

        unsafe { // TODO unneded unsafe? may be needed when writing past current len
            *self.slots_mut().get_unchecked_mut(pos) = Slot {
                offset: self.base.data_offset,
                key_len: key_len.try_into().expect("key too large"),
                head,
                payload_len
            };
        }

        self.base.sized_mut(self.base.data_offset, key_len).copy_from_slice(key_suffix);
        self.base.sized_mut(self.base.data_offset + key_len, payload_len).copy_from_slice(payload);
        assert!(self.base.data_offset >= std::mem::size_of::<Slot>() * self.base.len);
    }

    fn copy_key_value_range(&self, src_pos: usize, dst: &mut LeafNode, dst_pos: usize, amount: usize) -> error::Result<()> {
        let size_of_slot = std::mem::size_of::<Slot>();

        if self.base.prefix()? == dst.base.prefix().unopt() { // TODO shouldn't this compare the actual prefixes?
            // Fast path
            let dst_slots = LeafNode::slots_from_data_mut(&mut dst.base.data, dst.base.len + amount);
            unsafe {
                dst_slots
                    .get_unchecked_mut(dst_pos..dst_pos + amount)
                    .copy_from_slice(&self.slots()[src_pos..src_pos + amount]);
            }

            for i in 0..amount {
                let kv = self.kv(src_pos + i)?;
                let kv_len = kv.len();
                dst.base.data_offset -= kv_len;
                dst.base.space_used += kv_len;
                unsafe {
                    dst.slots_mut().get_unchecked_mut(dst_pos + i).offset = dst.base.data_offset;
                }
                dst.base.sized_mut(dst.base.data_offset, kv_len).copy_from_slice(kv);
            }
        } else {
            for i in 0..amount {
                let full_key = self.full_key_at(src_pos + i)?;
                let payload = self.value_at(src_pos + i)?;
                dst.store_key_value(dst_pos + i, full_key, payload);
            }
        }

        dst.base.len = dst_pos + amount; // TODO before it was len += amount, check?
        assert!(dst.base.data_offset >= size_of_slot * dst.base.len);
        // dst.check_node();
        Ok(())
    }

    #[inline(never)]
    pub(crate) fn compactify(&mut self) {
        // self.check_node();
        let space_after_compation = self.free_space_after_compaction();
        let mut tmp_node = BoxedNode::new(true, self.base.capacity);
        let tmp_leaf = tmp_node.as_leaf_mut();
        tmp_leaf.set_fences(self.base.lower_fence().unopt(), self.base.upper_fence().unopt());
        self.copy_key_value_range(0, tmp_leaf, 0, self.base.len).unopt();
        // tmp_leaf.upper_edge = self.upper_edge not needed on leaf
        tmp_node.copy_to(&mut self.base);
        self.make_hint();
        // self.check_node();
        assert!(self.free_space() == space_after_compation);
    }

    fn common_prefix_len(&self, a_pos: usize, b_pos: usize) -> error::Result<usize> {
        if self.base.len == 0 {
            Ok(0)
        } else {
            let key_a = self.key_at(a_pos)?;
            let key_b = self.key_at(b_pos)?;
            let limit = key_a.len().min(key_b.len());
            for i in 0..limit {
                if key_a[i] != key_b[i] {
                    return Ok(i);
                }
            };

            Ok(limit)
        }
    }

    // TODO fns find_separator, get_separator

    pub(crate) fn compare_key_with_boundaries<K: AsRef<[u8]>>(&self, key: K) -> error::Result<BoundaryOrdering> {
        if let Some(lower_fence) = self.base.lower_fence()? {
            if !(key.as_ref() > lower_fence) {
                // println!("{:?} before {:?}", key.as_ref(), lower_fence);
                return Ok(BoundaryOrdering::Before);
            }
        }
        if let Some(upper_fence) = self.base.upper_fence()? {
            if !(key.as_ref() <= upper_fence) {
                // println!("{:?} after {:?}", key.as_ref(), upper_fence);
                return Ok(BoundaryOrdering::After);
            }
        }

        return Ok(BoundaryOrdering::Within);
    }

    pub(crate) fn within_bounds<K: AsRef<[u8]>>(&self, key: K) -> error::Result<bool> {
        Ok(self.compare_key_with_boundaries(key)? == BoundaryOrdering::Within)
    }

    // actual removal is postponed until compaction
    fn remove_slot_at(&mut self, pos: usize) {
        self.base.space_used -= self.key_at(pos).unwrap().len() + self.value_at(pos).unwrap().len();
        self.slots_mut().copy_within(pos + 1.., pos);
        self.base.len -= 1;
        self.make_hint();
    }

    pub(crate) fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> bool {
        if let Some(pos) = self.lower_bound_exact(key.as_ref()).unopt() {
            self.remove_slot_at(pos);
            true
        } else {
            false
        }
    }

    pub(crate) fn remove_at(&mut self, pos: usize) {
        self.remove_slot_at(pos)
    }

    pub fn is_sorted(&self) -> bool {
        let mut sorted = true;
        for i in 0..self.base.len() - 1 {
            sorted &= self.key_at(i).unwrap() < self.key_at(i + 1).unwrap();
        }
        sorted
    }

//     pub (crate) fn check_node(&mut self) {
//         let len = self.base.len;
//         let mut prev_key = 0;
//         for i in 0..len {
//             let key = to_u64(self.full_key_at(i).unopt().as_slice());
//             let value = to_u64(self.value_at(i).unopt());
//             if key != value {
//                 panic!("key and value differ {} != {}", key, value);
//             }
//             if key != 0 && prev_key >= key {
//                 panic!("wrong ordering prev: {}, curr: {}", prev_key, key);
//             }
//             prev_key = key;
//         }
//     }

    // FIXME FIXED HERE, FIX THE TREE
    pub(crate) fn split_heuristic(&self, entry_hint: Option<SplitEntryHint>) -> error::Result<SplitStrategy> {
        let total_size = match entry_hint {
            Some(h) => self.used_capacity_after_compaction()
                + std::mem::size_of::<Slot>() + h.key.len() + h.value_len,
            None => self.used_capacity_after_compaction(),
        };

        if self.base.len < 2 {
            return Ok(SplitStrategy::Grow { new_size: total_size });
        }

        let mut left_used_space = 0;
        // let mut candidates = vec![];

        let n_split_candidates = 64;
        let split_step = self.base.len / n_split_candidates;

        type Split = ((f32, f32, f32, f32), (usize, usize, usize));
        let mut best_valid_split: Option<Split> = None;
        let mut best_split: Option<Split> = None;

        for i in 0..(self.base.len - 1) {
            let size = match entry_hint {
                Some(entry_hint) if entry_hint.pos == i => {
                    // FIXME(done?) there is no way currently to prevent the new entry to end in the same
                    // node as the entry of the hint position (from lower_bound), if those two
                    // entries are too large for the node, this could in theory cause a split
                    // recursion because we cannot split before 0 position

                    std::mem::size_of::<Slot>() + entry_hint.key.len() + entry_hint.value_len
                        + std::mem::size_of::<Slot>() + self.kv_len(i)?
                }
                _ => {
                    std::mem::size_of::<Slot>() + self.kv_len(i)?
                }
            };

            if i + 1 >= self.base.len || left_used_space + size > total_size { // FIXME improve ub detection
                // Read past len || size grew while computing heuristic
                // Unwinding
                return Err(error::Error::Unwind)
            }

            left_used_space += size;


//              if used_space + size <= self.base.capacity {
//                  used_space += size;
//              } else {
//                  break;
//              }

            if i % split_step == 0 {
                // TODO check if we should allow shrinking capacity
                let left_capacity = if left_used_space <= self.base.capacity {
                    self.base.capacity
                } else {
                    BufferManager::capacity_for::<Node>(left_used_space)
                };

                let left_utilization = left_used_space as f32 / left_capacity as f32 * 100.0;
                let left_capacity_ratio = left_capacity as f32 / self.base.capacity as f32;

                let right_used_space = total_size - left_used_space;

                let right_capacity = BufferManager::capacity_for::<Node>(right_used_space);
                let right_utilization = right_used_space as f32 / right_capacity as f32 * 100.0;
                let right_capacity_ratio = right_capacity as f32 / self.base.capacity as f32;

                let delta_utilization = (left_utilization - right_utilization).abs();
                let max_utilization = left_utilization.max(right_utilization);

                // candidates.push((left_capacity_ratio, right_capacity_ratio, delta_utilization, max_utilization, left_used_space, right_used_space, i));
                let candidate_split = ((left_capacity_ratio, right_capacity_ratio, delta_utilization, max_utilization), (left_used_space, right_used_space, i));

                if let Some(split) = best_split {
                    if candidate_split.0 < split.0 {
                        best_split = Some(candidate_split);
                    }
                } else {
                    best_split = Some(candidate_split);
                }

                if max_utilization < 90.0 {
                    if let Some(split) = best_valid_split {
                        if candidate_split.0 < split.0 {
                            best_valid_split = Some(candidate_split);
                        }
                    } else {
                        best_valid_split = Some(candidate_split);
                    }
                }

//                  println!("l_used = {}, r_used = {}", used_space, right_used_space);
//                  println!("l_util = {:.0}%, r_util = {:.0}%", left_utilization, right_utilization);
//                  println!("delta = {}, cap_ratio = {}", delta_utilization, capacity_ratio);
            }
        }


//          candidates
//              .sort_by(|a, b| (a.0, a.1, a.2, a.3)
//                       .partial_cmp(&(b.0, b.1, b.2, b.3)).unwrap());
//
//          let best_candidate = *candidates.iter()
//              .filter(|(_, _, _, max_util, _, _, _)| *max_util < 90.0)
//              .nth(0)
//              .unwrap_or(candidates.first().expect("must have one"));
//
//          let (_, _, _, _, left_node_size, right_node_size, split_pos) = best_candidate;

        let best_candidate = best_valid_split.or(best_split).expect("must have one");

        let (_, (left_node_size, right_node_size, split_pos)) = best_candidate;

        let key = self.full_key_at(split_pos)?;

        let new_left_size = if left_node_size > self.base.capacity {
            Some(left_node_size)
        } else {
            None
        };

//          #[cfg(debug_assertions)]
//          {
//              let lower = self.base.lower_fence()?.unwrap_or(&[]);
//              let upper = self.base.upper_fence()?.unwrap_or(&[]);
//              if key == lower || key == upper {
//                  println!("candidates: {:?}", candidates);
//                  println!("bad split position (at {}), will duplicate fences: key = {}, lower = {}, upper = {}", split_pos, to_u64(&key), to_u64(lower), to_u64(upper));
//
//                  panic!("at dup"); // FIXME needs recheck before this
//              }
//          }

        Ok(SplitStrategy::SplitAt { key, split_pos, new_left_size, right_size: right_node_size })
    }

    pub(crate) fn split_large(&mut self, right: &mut LeafNode, new_left: Option<&mut LeafNode>, strategy: &SplitStrategy) {
        let stored_len = self.base.len;

        match strategy {
            SplitStrategy::SplitAt { key, split_pos, new_left_size, right_size } => {

                match new_left {
                    Some(left) => {
                        let split_key = key.as_slice();

                        left.set_fences(self.base.lower_fence().unopt(), Some(split_key));
                        assert!(right.base.len == 0);
                        right.set_fences(Some(split_key), self.base.upper_fence().unopt());
                        self.copy_key_value_range(0, left, 0, split_pos + 1).unopt();
                        self.copy_key_value_range(split_pos + 1, right, 0, self.base.len - (split_pos + 1)).unopt();

                        left.make_hint();
                        right.make_hint();

                        // right.check_node();

                        // left.check_node();
                        assert!(stored_len == left.base.len + right.base.len);
                    }
                    None => {
                        // self.check_node();
                        let split_key = key.as_slice();

                        let mut left_node = BoxedNode::new(true, self.base.capacity);
                        let left = left_node.as_leaf_mut();
                        left.set_fences(self.base.lower_fence().unopt(), Some(split_key));
                        assert!(right.base.len == 0);
                        right.set_fences(Some(split_key), self.base.upper_fence().unopt());
                        self.copy_key_value_range(0, left, 0, split_pos + 1).unopt();
                        self.copy_key_value_range(split_pos + 1, right, 0, self.base.len - (split_pos + 1)).unopt();

                        left.make_hint();
                        right.make_hint();

                        // right.check_node();

                        left_node.copy_to(&mut self.base);
                        // self.check_node();
                        assert!(stored_len == self.base.len + right.base.len);
                    }
                }
            },
            _ => {
                unreachable!("must be a split");
            }
        }
    }

    pub(crate) fn copy_to(&mut self, other: &mut LeafNode) {
        let stored_len = self.base.len;

        other.set_fences(self.base.lower_fence().unopt(), self.base.upper_fence().unopt());
        debug_assert!(other.base.len == 0);
        self.copy_key_value_range(0, other, 0, self.base.len).unopt();

        other.make_hint();

        assert!(stored_len == other.base.len);
    }

    pub(crate) fn split_pre(&mut self, right: &mut LeafNode, split_meta: &SplitMeta) {
        let stored_len = self.base.len;
        // self.check_node();
        let (split_key, split_pos) = match split_meta {
            SplitMeta::AtPos { key, at } => (key.as_slice(), at + 1),
            SplitMeta::BeforePos { key, before } => (key.as_slice(), *before)
        };
        let mut left_node = BoxedNode::new(true, self.base.capacity);
        let left = left_node.as_leaf_mut();
        left.set_fences(self.base.lower_fence().unopt(), Some(split_key));
        assert!(right.base.len == 0);
        right.set_fences(Some(split_key), self.base.upper_fence().unopt());
        self.copy_key_value_range(0, left, 0, split_pos).unopt();
        self.copy_key_value_range(split_pos, right, 0, self.base.len - split_pos).unopt();

        left.make_hint();
        right.make_hint();

        // right.check_node();

        left_node.copy_to(&mut self.base);
        // self.check_node();
        assert!(stored_len == self.base.len + right.base.len);
    }

    pub(crate) fn split(&mut self, right: &mut LeafNode, split_pos: usize) {
        let stored_len = self.base.len;
        // self.check_node();
        let split_key = self.full_key_at(split_pos).unopt();
        let mut left_node = BoxedNode::new(true, self.base.capacity);
        let left = left_node.as_leaf_mut();
        left.set_fences(self.base.lower_fence().unopt(), Some(&split_key));
        assert!(right.base.len == 0);
        right.set_fences(Some(&split_key), self.base.upper_fence().unopt());
        self.copy_key_value_range(0, left, 0, split_pos + 1).unopt();
        self.copy_key_value_range(split_pos + 1, right, 0, self.base.len - (split_pos + 1)).unopt();

        left.make_hint();
        right.make_hint();

        // right.check_node();

        left_node.copy_to(&mut self.base);
        // self.check_node();
        assert!(stored_len == self.base.len + right.base.len);
    }

    #[inline]
    pub(crate) fn can_merge_with(&self, right: &LeafNode) -> bool {
        let left = self;
        let new_prefix_len = Node::calculate_prefix_len(
            left.base.lower_fence().unopt(),
            right.base.upper_fence().unopt()
            );

        let left_grow = (left.base.prefix_len - new_prefix_len) * left.base.len;
        let right_grow = (right.base.prefix_len - new_prefix_len) * right.base.len;

        let space_upper_bound = left.base.space_used + right.base.space_used
            + (std::mem::size_of::<Slot>() * (left.base.len + right.base.len))
            + left_grow + right_grow;

        space_upper_bound <= left.base.capacity
    }

    pub(crate) fn merge(&mut self, right: &mut LeafNode) -> bool {
        // self.check_node();
        // right.check_node();

        // FIXME determine if we should reject merging if the right node is smaller than ourselves,
        // so that the larger nodes get reclaimed eventualy

        if !self.can_merge_with(&right) {
            return false;
        }

        // TODO check if we should allocate a node with a different capacity (done: we cannot
        // allocate different sizes here)

        let mut tmp_node = BoxedNode::new(true, self.base.capacity);
        let tmp = tmp_node.as_leaf_mut();
        tmp.set_fences(self.base.lower_fence().unopt(), right.base.upper_fence().unopt());

        self.copy_key_value_range(0, tmp, 0, self.base.len).unopt();
        right.copy_key_value_range(0, tmp, self.base.len, right.base.len).unopt();

        tmp.make_hint();
        tmp_node.copy_to(&mut self.base);

        // self.check_node();
        true
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(C)]
struct InnerSlot {
    offset: usize,
    key_len: u32,
    head: u32,
    payload: Swip<HybridLatch<BufferFrame>>
}

const EDGE_LEN: usize = std::mem::size_of::<Swip<HybridLatch<BufferFrame>>>();

#[repr(C)]
pub struct InternalNode {
    pub(crate) base: Node,
    data: ()
}

impl InternalNode {
    fn free_space(&self) -> usize {
        self.base.capacity - (self.base.capacity - self.base.data_offset) - (std::mem::size_of::<InnerSlot>() * self.base.len)
    }

    fn free_space_after_compaction(&self) -> usize {
        self.base.capacity - self.base.space_used - (std::mem::size_of::<InnerSlot>() * self.base.len)
    }

    fn used_capacity_after_compaction(&self) -> usize {
        self.base.space_used + (std::mem::size_of::<InnerSlot>() * self.base.len)
    }

    pub(crate) fn has_enough_space_for(&self, space_needed: usize) -> bool {
        space_needed <= self.free_space() || space_needed <= self.free_space_after_compaction()
    }

    fn request_space_for(&mut self, space_needed: usize) -> bool {
        if space_needed <= self.free_space() {
            true
        } else if space_needed <= self.free_space_after_compaction() {
            self.compactify();
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn is_underfull(&self) -> bool {
        self.free_space_after_compaction() < (self.base.capacity as f32 * 0.4) as usize
    }

    fn slots(&self) -> &[InnerSlot] {
        unsafe { slice::from_raw_parts(std::ptr::addr_of!(self.base.data) as *const InnerSlot, self.base.len) }
    }

    fn slots_mut(&mut self) -> &mut [InnerSlot] {
        unsafe { slice::from_raw_parts_mut(std::ptr::addr_of_mut!(self.base.data) as *mut InnerSlot, self.base.len) }
    }

    fn slots_with_len_mut(&mut self, new_len: usize) -> &mut [InnerSlot] {
        unsafe { slice::from_raw_parts_mut(std::ptr::addr_of_mut!(self.base.data) as *mut InnerSlot, new_len) }
    }

    fn slots_from_data_mut(data: &mut Data, new_len: usize) -> &mut [InnerSlot] {
        unsafe { slice::from_raw_parts_mut(data as *mut Data as *mut InnerSlot, new_len) }
    }

    #[inline]
    pub(crate) fn key_at(&self, pos: usize) -> error::Result<&[u8]> {
        let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
        Ok(self.base.sized(slot.offset, slot.key_len as usize)?)
    }

    #[inline]
    fn full_key_len(&self, pos: usize) -> error::Result<usize> {
        let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
        Ok(self.base.prefix_len + slot.key_len as usize)
    }

    #[inline]
    pub(crate) fn full_key_at(&self, pos: usize) -> error::Result<Vec<u8>> {
        let prefix = self.base.prefix()?;
        let suffix = self.key_at(pos)?;
        let mut out = Vec::with_capacity(prefix.len() + suffix.len());
        out.extend_from_slice(prefix);
        out.extend_from_slice(suffix);
        Ok(out)
    }

    #[inline]
    pub(crate) fn copy_full_key_at(&self, pos: usize, buffer: &mut Vec<u8>) -> error::Result<()> {
        let prefix = self.base.prefix()?;
        let suffix = self.key_at(pos)?;
        buffer.truncate(0);
        buffer.extend_from_slice(prefix);
        buffer.extend_from_slice(suffix);
        Ok(())
    }

    #[inline]
    pub(crate) fn edge_at(&self, pos: usize) -> error::Result<&Swip<HybridLatch<BufferFrame>>> {
        if pos == self.base.len {
            self.upper_edge()
        } else {
            let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
            Ok(&slot.payload)
        }
    }

    pub(crate) fn replace_edge_at(&mut self, pos: usize, new_edge: Swip<HybridLatch<BufferFrame>>) -> Swip<HybridLatch<BufferFrame>> {
        if pos == self.base.len {
            panic!("use replace upper_edge instead");
        } else {
            let slot = unsafe { self.slots_mut().get_unchecked_mut(pos) }; // TODO maybe check bounds?
            std::mem::replace(&mut slot.payload, new_edge)
        }
    }

    pub(crate) fn upper_edge(&self) -> error::Result<&Swip<HybridLatch<BufferFrame>>> {
        match self.base.upper_edge.as_ref() {
            Some(edge) => Ok(edge),
            None => Err(error::Error::Unwind)
        }
    }

    pub(crate) fn replace_upper_edge(&mut self, new_edge: Swip<HybridLatch<BufferFrame>>) -> Swip<HybridLatch<BufferFrame>> {
        self.base.upper_edge.replace(new_edge).expect("internal node must have upper edge")
    }

    pub(crate) fn set_upper_edge(&mut self, new_edge: Swip<HybridLatch<BufferFrame>>) {
        let res = self.base.upper_edge.replace(new_edge);
        debug_assert!(res.is_none());
    }

//     #[inline]
//     fn kv_len(&self, pos: usize) -> error::Result<usize> {
//         let slot = unsafe { self.slots().get_unchecked(pos) }; // TODO maybe check bounds?
//         Ok(slot.key_len as usize + slot.payload_len)
//     }

    #[inline]
    pub(crate) fn lower_bound_exact<K: AsRef<[u8]>>(&self, key: K) -> error::Result<Option<usize>> {
        let key = key.as_ref();
        if (key.len() < self.base.prefix_len) || &key[..self.base.prefix_len] != self.base.prefix()? {
            return Ok(None);
        }

        let (pos, exact) = self.lower_bound_suffix(unsafe { key.get_unchecked(self.base.prefix_len..) })?;

        if exact {
            Ok(Some(pos))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn lower_bound<K: AsRef<[u8]>>(&self, key: K) -> error::Result<(usize, bool)> {
        use std::cmp::Ordering;

//          let first_hint = self.base.hints[0];
//          if self.base.hints.iter().all(|&h| h == first_hint) {
//              return self.lower_bound_bench(key);
//          }

        let key = key.as_ref();
        let cmp_len = key.len().min(self.base.prefix_len);
        match unsafe { key.get_unchecked(..cmp_len).cmp(self.base.prefix()?) } {
            Ordering::Less => {
                return Ok((0, false));
            }
            Ordering::Greater => {
                return Ok((self.base.len, false));
            }
            Ordering::Equal => {}
        }

        self.lower_bound_suffix(unsafe { key.get_unchecked(self.base.prefix_len..) })
    }

    #[inline]
    pub fn lower_bound_bench<K: AsRef<[u8]>>(&self, key: K) -> error::Result<(usize, bool)> {
        use std::cmp::Ordering;

        let key = key.as_ref();
        let cmp_len = key.len().min(self.base.prefix_len);
        match unsafe { key.get_unchecked(..cmp_len).cmp(self.base.prefix()?) } {
            Ordering::Less => {
                return Ok((0, false));
            }
            Ordering::Greater => {
                return Ok((self.base.len, false));
            }
            Ordering::Equal => {}
        }

        let suffix = unsafe { key.get_unchecked(self.base.prefix_len..) };

        let mut lower = 0;
        let mut upper = self.base.len;

        while lower < upper {
            let mid = ((upper - lower) / 2) + lower;

            let mid_key = self.key_at(mid)?;
            if suffix < mid_key {
                upper = mid;
            } else if suffix > mid_key {
                lower = mid + 1;
            } else {
                return Ok((mid, true));
            }
        }

        Ok((lower, false))
    }

    #[inline]
    fn lower_bound_suffix<K: AsRef<[u8]>>(&self, key: K) -> error::Result<(usize, bool)> {
        let key = key.as_ref();
        let mut lower = 0;
        let mut upper = self.base.len;
        let head = to_head(key);

        if self.base.len > HINT_COUNT * 2 {
            let dist = self.base.len / (HINT_COUNT + 1);
            let (pos1, pos2) = self.base.search_hint(head);
            lower = pos1 * dist;
            if pos2 < HINT_COUNT {
                upper = (pos2 + 1) * dist;
            }
        }

        let slots = self.slots();

        while lower < upper {
            let mid = ((upper - lower) / 2) + lower;

            let mid_slot = unsafe { slots.get_unchecked(mid) };

            if head < mid_slot.head {
                upper = mid;
            } else if head > mid_slot.head {
                lower = mid + 1;
            } else if mid_slot.key_len <= 4 {
                // head is equal, we don't have to check the rest of the key
                if key.len() < mid_slot.key_len as usize {
                    upper = mid;
                } else if key.len() > mid_slot.key_len as usize {
                    lower = mid + 1;
                } else {
                    return Ok((mid, true));
                }
            } else {
                let mid_key = self.key_at(mid)?;
                if key < mid_key {
                    upper = mid;
                } else if key > mid_key {
                    lower = mid + 1;
                } else {
                    return Ok((mid, true));
                }
            }
        }

        Ok((lower, false))
    }

    fn make_hint(&mut self) {
        let dist = self.base.len / (HINT_COUNT + 1);
        for i in 0..HINT_COUNT {
            unsafe {
                *self.base.hints.get_unchecked_mut(i) = self.slots().get_unchecked(dist * (i + 1)).head;
            }
        }
    }

    fn update_hint(&mut self, pos: usize) {
        let dist = self.base.len / (HINT_COUNT + 1);
        let mut start = 0;
        if (self.base.len > (HINT_COUNT * 2 + 1))
            && ((self.base.len - 1) / (HINT_COUNT + 1) == dist)
            && (pos / dist > 1)
        {
            start = (pos / dist) - 1;
        }

        for i in start..HINT_COUNT {
            unsafe {
                *self.base.hints.get_unchecked_mut(i) = self.slots().get_unchecked(dist * (i + 1)).head;
            }
        }

        // TODO remove check?
//          for i in 0..HINT_COUNT {
//              unsafe {
//                  assert_eq!(*self.base.hints.get_unchecked(i), self.slots().get_unchecked(dist * (i + 1)).head);
//              }
//          }
    }

    fn space_needed_with_prefix_len(&self, key_len: usize, prefix_len: usize) -> usize {
        std::mem::size_of::<InnerSlot>() + (key_len - prefix_len) // + payload_len: payload already in slot
    }

    pub(crate) fn space_needed(&self, key_len: usize) -> usize {
        self.space_needed_with_prefix_len(key_len, self.base.prefix_len)
    }

    pub(crate) fn can_insert(&self, key_len: usize) -> bool {
        let space_needed = self.space_needed(key_len);
        self.has_enough_space_for(space_needed)
    }

    fn prepare_insert(&mut self, key_len: usize) -> bool {
        let space_needed = self.space_needed(key_len);
        if !self.request_space_for(space_needed) {
            false
        } else {
            true
        }
    }

    // TODO insert_reserve_payload

    pub(crate) fn insert<K: AsRef<[u8]>>(&mut self, key: K, payload: Swip<HybridLatch<BufferFrame>>) -> Option<usize> {
        debug_assert!(self.can_insert(key.as_ref().len()));
        if !self.prepare_insert(key.as_ref().len()) {
            return None;
        }

        let (pos, _) = self.lower_bound(key.as_ref()).unopt();
        let curr_len = self.base.len;
        let new_slots = self.slots_with_len_mut(self.base.len + 1);
        new_slots.copy_within(pos..curr_len, pos + 1);
        self.store_key_value(pos, key, payload); // TODO This may write past slots len, consider changing len before this
        self.base.len += 1;
        self.update_hint(pos);
        Some(pos)
    }

    fn set_lower_fence<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        let key_len = key.len();
        assert!(self.free_space() >= key_len);
        self.base.data_offset -= key_len;
        self.base.space_used += key_len;
        self.base.lower_fence = Fence {
            offset: self.base.data_offset,
            len: key_len
        };
        self.base.sized_mut(self.base.data_offset, key_len).copy_from_slice(key);
    }

    fn set_upper_fence<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();
        let key_len = key.len();
        assert!(self.free_space() >= key_len);
        self.base.data_offset -= key_len;
        self.base.space_used += key_len;
        self.base.upper_fence = Fence {
            offset: self.base.data_offset,
            len: key_len
        };
        self.base.sized_mut(self.base.data_offset, key_len).copy_from_slice(key);
    }

    fn set_fences(&mut self, lower_fence_key: Option<&[u8]>, upper_fence_key: Option<&[u8]>) {
        let lower_key = if let Some(lower_fence_key) = lower_fence_key {
            self.set_lower_fence(lower_fence_key);
            lower_fence_key
        } else {
            &[]
        };

        let upper_key = if let Some(upper_fence_key) = upper_fence_key {
            self.set_upper_fence(upper_fence_key);
            upper_fence_key
        } else {
            &[]
        };

        let mut prefix_len = 0;
        while (prefix_len < lower_key.len().min(upper_key.len())) && (lower_key[prefix_len] == upper_key[prefix_len])
        {
            prefix_len += 1;
        }

        self.base.prefix_len = prefix_len;
    }

    fn store_key_value<K: AsRef<[u8]>>(&mut self, pos: usize, key: K, payload: Swip<HybridLatch<BufferFrame>>) {
        use std::convert::TryInto;

        let key = key.as_ref();

        let key_suffix = &key[self.base.prefix_len..];

        let key_len = key_suffix.len();

        let head = to_head(key_suffix);
        let full_size = key_len;
        self.base.data_offset -= full_size;
        self.base.space_used += full_size;

        unsafe { // TODO unneded unsafe? may be needed when writing past current len
            *self.slots_mut().get_unchecked_mut(pos) = InnerSlot {
                offset: self.base.data_offset,
                key_len: key_len.try_into().expect("key too large"),
                head,
                payload
            };
        }

        self.base.sized_mut(self.base.data_offset, key_len).copy_from_slice(key_suffix);
        assert!(self.base.data_offset >= std::mem::size_of::<InnerSlot>() * self.base.len);
    }

    fn copy_key_value_range(&self, src_pos: usize, dst: &mut InternalNode, dst_pos: usize, amount: usize) -> error::Result<()> {
        let size_of_slot = std::mem::size_of::<InnerSlot>();

        if self.base.prefix()? == dst.base.prefix().unopt() { // TODO shouldn't this compare the actual prefixes?
            // Fast path
            let dst_slots = InternalNode::slots_from_data_mut(&mut dst.base.data, dst.base.len + amount);
            unsafe {
                dst_slots
                    .get_unchecked_mut(dst_pos..dst_pos + amount)
                    .copy_from_slice(&self.slots()[src_pos..src_pos + amount]);
            }

            for i in 0..amount {
                let key = self.key_at(src_pos + i)?;
                let key_len = key.len();
                dst.base.data_offset -= key_len;
                dst.base.space_used += key_len;

                unsafe {
                    dst.slots_mut().get_unchecked_mut(dst_pos + i).offset = dst.base.data_offset;
                }
                dst.base.sized_mut(dst.base.data_offset, key_len).copy_from_slice(key);
            }
        } else {
            for i in 0..amount {
                let full_key = self.full_key_at(src_pos + i)?;
                let payload = self.edge_at(src_pos + i)?;
                dst.store_key_value(dst_pos + i, full_key, payload.clone());
            }
        }

        dst.base.len = dst_pos + amount;
        assert!(dst.base.data_offset >= size_of_slot * dst.base.len);
        // dst.check_node();
        Ok(())
    }

    pub(crate) fn compactify(&mut self) {
        let space_after_compation = self.free_space_after_compaction();
        let mut tmp_node = BoxedNode::new(false, self.base.capacity);
        let tmp_internal = tmp_node.as_internal_mut();
        tmp_internal.set_fences(self.base.lower_fence().unwrap(), self.base.upper_fence().unwrap());
        self.copy_key_value_range(0, tmp_internal, 0, self.base.len).unopt();
        tmp_internal.base.upper_edge = self.base.upper_edge;
        tmp_node.copy_to(&mut self.base);
        self.make_hint();
        assert!(self.free_space() == space_after_compation);
    }

    fn common_prefix_len(&self, a_pos: usize, b_pos: usize) -> error::Result<usize> {
        if self.base.len == 0 {
            Ok(0)
        } else {
            let key_a = self.key_at(a_pos)?;
            let key_b = self.key_at(b_pos)?;
            let limit = key_a.len().min(key_b.len());
            for i in 0..limit {
                if key_a[i] != key_b[i] {
                    return Ok(i);
                }
            };

            Ok(limit)
        }
    }

    // TODO fns find_separator, get_separator

    pub(crate) fn compare_key_with_boundaries<K: AsRef<[u8]>>(&self, key: K) -> error::Result<BoundaryOrdering> {
        if let Some(lower_fence) = self.base.lower_fence()? {
            if !(key.as_ref() > lower_fence) {
                return Ok(BoundaryOrdering::Before);
            }
        }
        if let Some(upper_fence) = self.base.upper_fence()? {
            if !(key.as_ref() <= upper_fence) {
                return Ok(BoundaryOrdering::After);
            }
        }

        return Ok(BoundaryOrdering::Within);
    }

    // actual removal is postponed until compaction
    fn remove_slot_at(&mut self, pos: usize) {
        self.base.space_used -= self.key_at(pos).unwrap().len();
        self.slots_mut().copy_within(pos + 1.., pos);
        self.base.len -= 1;
        self.make_hint();
    }

    pub(crate) fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> bool {
        if let Some(pos) = self.lower_bound_exact(key.as_ref()).unopt() {
            self.remove_slot_at(pos);
            true
        } else {
            false
        }
    }

    pub(crate) fn remove_at(&mut self, pos: usize) {
        self.remove_slot_at(pos)
    }

    pub(crate) fn remove_edge_at(&mut self, pos: usize) -> Swip<HybridLatch<BufferFrame>> {
        let slot = unsafe { self.slots_mut().get_unchecked_mut(pos) }; // TODO maybe check bounds?
        let payload = slot.payload.clone();
        drop(slot);
        self.remove_slot_at(pos);
        payload
    }

//     pub (crate) fn check_node(&mut self) {
//         let len = self.base.len;
//         let mut prev_key = 0;
//         for i in 0..len {
//             let key = to_u64(self.full_key_at(i).unopt().as_slice());
//             if prev_key >= key {
//                 panic!("wrong ordering");
//             }
//             prev_key = key;
//         }
//     }

    pub(crate) fn split(&mut self, right: &mut InternalNode, split_pos: usize) {
        // self.check_node();
        let split_key = self.full_key_at(split_pos).unopt();
        let mut left_node = BoxedNode::new(false, self.base.capacity);
        let left = left_node.as_internal_mut();
        left.set_fences(self.base.lower_fence().unopt(), Some(&split_key));
        assert!(right.base.len == 0);
        right.set_fences(Some(&split_key), self.base.upper_fence().unopt());

        self.copy_key_value_range(0, left, 0, split_pos).unopt();
        self.copy_key_value_range(split_pos + 1, right, 0, self.base.len - (split_pos + 1)).unopt();
        left.base.upper_edge = Some(self.edge_at(split_pos).unopt().clone());
        right.base.upper_edge = self.base.upper_edge.clone();

        left.make_hint();
        right.make_hint();

        left_node.copy_to(&mut self.base);
        // self.check_node();
    }

    #[inline]
    pub(crate) fn can_merge_with(&self, right: &InternalNode) -> bool {
        let left = self;
        let new_prefix_len = Node::calculate_prefix_len(
            left.base.lower_fence().unopt(),
            right.base.upper_fence().unopt()
        );

        let left_grow = (left.base.prefix_len - new_prefix_len) * left.base.len;
        let right_grow = (right.base.prefix_len - new_prefix_len) * right.base.len;

        let extra_key_len = right.base.lower_fence.len;

        let space_upper_bound = left.base.space_used + right.base.space_used
            + (std::mem::size_of::<InnerSlot>() * (left.base.len + right.base.len))
            + left_grow + right_grow
            + extra_key_len;

        space_upper_bound <= left.base.capacity
    }

    pub(crate) fn merge(&mut self, right: &mut InternalNode) -> bool {
        // self.check_node();

        if !self.can_merge_with(&right) {
            return false;
        }

        let mut tmp_node = BoxedNode::new(false, self.base.capacity);
        let tmp = tmp_node.as_internal_mut();
        tmp.set_fences(self.base.lower_fence().unopt(), right.base.upper_fence().unopt());
        let extra_key = right.base.lower_fence().unopt().expect("lower fence must exist");

        // Copy left contents
        self.copy_key_value_range(0, tmp, 0, self.base.len).unopt();

        // Extra key insertion
        tmp.store_key_value(self.base.len, extra_key, self.base.upper_edge.clone().expect("left must have upper edge"));
        tmp.base.len += 1;

        // Copy right contents
        right.copy_key_value_range(0, tmp, tmp.base.len, right.base.len).unopt();
        tmp.base.upper_edge = right.base.upper_edge.clone();

        tmp.make_hint();
        tmp_node.copy_to(&mut self.base);

        // self.check_node();
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::NonOptimisticExt, persistent::node::SplitMeta};

    use super::{Node, LeafNode, BoxedNode, Slot, SplitEntryHint};

    #[test]
    fn persistent_leaf_node_insert() {
        let mut node = BoxedNode::new(true, 1000);
        let leaf = node.as_leaf_mut();

        leaf.insert(b"0001", b"1").unwrap();
        leaf.insert(b"0002", b"2").unwrap();
        leaf.insert(b"0004", b"4").unwrap();

        assert!(leaf.lower_bound(b"0001").unwrap() == (0, true));
        assert!(leaf.lower_bound(b"0002").unwrap() == (1, true));
        assert!(leaf.lower_bound(b"00002").unwrap() == (0, false));
        assert!(leaf.lower_bound(b"0005").unwrap() == (3, false));
        assert!(leaf.lower_bound(b"0003").unwrap() == (2, false));

        assert_eq!(leaf.value_at(0).unwrap(), b"1");

        assert!(leaf.remove(b"0001"));
        assert!(leaf.remove(b"0002"));
        assert!(leaf.remove(b"0004"));

        assert!(!leaf.remove(b"0005"));

        assert!(leaf.base.len == 0);

        assert!(leaf.lower_bound(b"0001").unwrap() == (0, false));
    }

    #[test]
    fn persistent_leaf_node_split_heuristic() {
        let mut node = BoxedNode::new(true, 128 * 1024);
        let leaf = node.as_leaf_mut();

        for i in 0..9 {
            let key_len = 8;
            let val_len = if i == 8 {
                (8 * 8192) - (key_len + std::mem::size_of::<Slot>())
            } else {
                8192 - (key_len + std::mem::size_of::<Slot>())
            };
            leaf.insert(format!("{:08}", i).as_bytes(), b"A".repeat(val_len)).unwrap();
        }

        let result = leaf.split_heuristic(None).unopt();

        println!("result = {:?}", result);
        // TODO assert_eq!((SplitMeta::AtPos { key: b"00000007".to_vec(), at: 7 }, 65536), result);

        println!("With entry hint");
        let mut node = BoxedNode::new(true, 128 * 1024);
        let leaf = node.as_leaf_mut();

        for i in 0..9 {
            let key_len = 8;
            let val_len = if i == 8 {
                (8 * 8192) - (key_len + std::mem::size_of::<Slot>())
            } else {
                8192 - (key_len + std::mem::size_of::<Slot>())
            };
            leaf.insert(format!("{:08}", i).as_bytes(), b"A".repeat(val_len)).unwrap();
        }

        let result = leaf.split_heuristic(Some(SplitEntryHint {
            pos: 0, key: b"00000000", value_len: (8 * 8192) - (8 + std::mem::size_of::<Slot>())
        })).unopt();

        println!("result = {:?}", result);
        // TODO assert_eq!((SplitMeta::AtPos { key: b"00000003".to_vec(), at: 3 }, 98280), result);

        println!("Simple split");
        let mut node = BoxedNode::new(true, 64 * 1024);
        let leaf = node.as_leaf_mut();

        for i in 0..8 {
            let key_len = 8;
            let val_len = 8192 - (key_len + std::mem::size_of::<Slot>());
            leaf.insert(format!("{:08}", i).as_bytes(), b"A".repeat(val_len)).unwrap();
        }

        let result = leaf.split_heuristic(Some(SplitEntryHint {
            pos: 0,
            key: b"00000000",
            value_len: 8192 - (8 + std::mem::size_of::<Slot>())
        })).unopt();

        println!("result = {:?}", result);
        // TODO assert_eq!((SplitMeta::AtPos { key: b"00000002".to_vec(), at: 2 }, 40936), result);
    }
}
