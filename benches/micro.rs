use bplustree::persistent::new_leaked_bufmgr;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use bplustree::persistent::PersistentBPlusTree;
use bplustree::persistent::ensure_global_bufmgr;
use bplustree::GenericBPlusTree;

use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[inline(never)]
fn stop_here () {
    println!("here");
}

fn lookup(c: &mut Criterion) {
    use rand::prelude::*;
    use rand::seq::SliceRandom;
    use rand::rngs::SmallRng;

    ensure_global_bufmgr("/dev/shm/state.db", 100 * 1024 * 1024).unwrap();

    let empty = false;
    let n_ops = 1000000 * 1;
    println!("ops: {}", n_ops);

    type MapKV = [u8; 8];

    let treeindex: Arc<PersistentBPlusTree> = PersistentBPlusTree::new_registered();
    let treeindex_inmem: Arc<GenericBPlusTree<MapKV, MapKV, 1536, 1536>> = Arc::new(GenericBPlusTree::new());
    let mut btreemap: BTreeMap<MapKV, MapKV> = BTreeMap::new();

    if !empty {
        let mut rng = SmallRng::seed_from_u64(39931002);
        let mut data: Vec<usize> = (0..n_ops).collect();
        data.shuffle(&mut rng);

        for i in 0..n_ops {
            let mut iter = treeindex.raw_iter_mut();
            let k = data[i].to_le_bytes();
            let v =  data[i].to_le_bytes();
            iter.insert(k, v);
            let mut iter = treeindex_inmem.raw_iter_mut();
            iter.insert(MapKV::from(k), MapKV::from(v));
            btreemap.insert(MapKV::from(k), MapKV::from(v));
        }
    }

    stop_here();

    let bytes = 303200usize.to_le_bytes();
    let bytes_slice = bytes.as_slice();

    c.bench_function("persistent find_leaf", |b| b.iter(|| {
        let _ = treeindex.find_leaf(black_box(bytes_slice)).unwrap();
    }));

    let (leaf, parent_opt) = treeindex.find_leaf_and_parent(black_box(bytes_slice)).unwrap();
    let (parent, pos) = parent_opt.unwrap();
    println!("parent_len: {}, leaf_len: {}", parent.len(), leaf.len());
    c.bench_function("persistent internal lowerbound", |b| b.iter(|| {
        let (pos, _) = parent.as_internal().lower_bound(black_box(bytes_slice)).unwrap();
    }));

    c.bench_function("persistent leaf lowerbound", |b| b.iter(|| {
        let (pos, _) = leaf.as_leaf().lower_bound(black_box(bytes_slice)).unwrap();
    }));

//      c.bench_function("find_leaf", |b| b.iter(|| {
//          let _ = treeindex_inmem.find_leaf(black_box(&303200usize)).unwrap();
//      }));



     c.bench_function("persistent lookup", |b| b.iter(|| {
         let _ = treeindex.lookup(black_box(bytes_slice), |_| ());
     }));

     c.bench_function("lookup", |b| b.iter(|| {
         let _ = treeindex_inmem.lookup(black_box(bytes_slice), |_| ());
     }));

     c.bench_function("btreemap lookup", |b| b.iter(|| {
         let _ = btreemap.get(black_box(bytes_slice));
     }));
}

fn end_to_end(c: &mut Criterion) {
    use rand::prelude::*;
    use rand::seq::SliceRandom;
    use rand::rngs::SmallRng;

    let bufmgr = new_leaked_bufmgr("/dev/shm/state.db", 10 * 1024 * 1024).unwrap();

    let empty = false;
    let n_ops = 10000000 * 1;

    type MapKV = [u8; 8];

    let mut btreemap: BTreeMap<MapKV, MapKV> = BTreeMap::new();


    c.bench_function("persistent insert", |b| {
        b.iter_custom(|iters| {
            let n_ops = iters as usize;
            let mut rng = SmallRng::seed_from_u64(39931002);
            let mut data: Vec<usize> = (0..n_ops).collect();
            data.shuffle(&mut rng);

            let treeindex: Arc<PersistentBPlusTree> = PersistentBPlusTree::new_registered_with(bufmgr);

            let start = Instant::now();
            for i in 0..iters {
                let mut iter = treeindex.raw_iter_mut();
                let k = data[i as usize].to_le_bytes();
                let v =  data[i as usize].to_le_bytes();
                iter.insert(k, v);
            }
            start.elapsed()
        })
    });

    c.bench_function("insert", |b| {
        b.iter_custom(|iters| {
            let n_ops = iters as usize;
            let mut rng = SmallRng::seed_from_u64(39931002);
            let mut data: Vec<usize> = (0..n_ops).collect();
            data.shuffle(&mut rng);

            let treeindex_inmem: Arc<GenericBPlusTree<MapKV, MapKV, 1536, 1536>> = Arc::new(GenericBPlusTree::new());

            let start = Instant::now();
            for i in 0..iters {
                let mut iter = treeindex_inmem.raw_iter_mut();
                let k = data[i as usize].to_le_bytes();
                let v =  data[i as usize].to_le_bytes();
                iter.insert(k, v);
            }
            start.elapsed()
        })
    });

    c.bench_function("btreemap insert", |b| {
        b.iter_custom(|iters| {
            let n_ops = iters as usize;
            let mut rng = SmallRng::seed_from_u64(39931002);
            let mut data: Vec<usize> = (0..n_ops).collect();
            data.shuffle(&mut rng);

            let mut btreemap: BTreeMap<MapKV, MapKV> = BTreeMap::new();

            let start = Instant::now();
            for i in 0..iters {
                let k = data[i as usize].to_le_bytes();
                let v =  data[i as usize].to_le_bytes();
                btreemap.insert(MapKV::from(k), MapKV::from(v));
            }
            start.elapsed()
        })
    });

//      if !empty {
//          let mut rng = SmallRng::seed_from_u64(39931002);
//          let mut data: Vec<usize> = (0..n_ops).collect();
//          data.shuffle(&mut rng);
//
//          for i in 0..n_ops {
//              let mut iter = treeindex.raw_iter_mut();
//              let k = data[i].to_le_bytes();
//              let v =  data[i].to_le_bytes();
//              iter.insert(k, v);
//              let mut iter = treeindex_inmem.raw_iter_mut();
//              iter.insert(MapKV::from(k), MapKV::from(v));
//              btreemap.insert(MapKV::from(k), MapKV::from(v));
//          }
//      }


}


criterion_group!(benches, lookup, end_to_end);
criterion_main!(benches);
