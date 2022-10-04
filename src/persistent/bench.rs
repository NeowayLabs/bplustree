#[cfg(test)]
mod benchmark {
    use crate::tree::PersistentBPlusTree;
    use crate::ensure_global_bufmgr;
    use serial_test::serial;

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    #[test]
    #[serial]
    fn persistent_weighted() {
        use rand::prelude::*;
        use rand::distributions::WeightedIndex;
        use rand::seq::SliceRandom;

        ensure_global_bufmgr("/tmp/state.db", 50 * 1024 * 1024).unwrap();

        let empty = false;
        let n_ops = 7000000 * 1;
        let n_threads = 7;
        let ops_per_thread = n_ops / n_threads;
        let weights = [20.0, 20.0, 55.0, 5.0, 0.00];
        // let weights = [00.0, 100.0, 0.0, 0.0, 0.00];

        println!("{}", ops_per_thread);

        let barrier = Arc::new(Barrier::new(n_threads + 1));
        let total_num_operations = Arc::new(AtomicUsize::new(0));

        let treeindex: Arc<PersistentBPlusTree> = PersistentBPlusTree::new_registered();

        if !empty {
            let mut rng = thread_rng();
            let mut data: Vec<usize> = (0..ops_per_thread).collect();
            data.shuffle(&mut rng);

            println!("Started insertion");
            for i in 0..ops_per_thread {
                let mut iter = treeindex.raw_iter_mut();
                iter.insert(data[i].to_be_bytes(), data[i].to_be_bytes());
            }
        }

        let mut handles = vec![];
        for _ in 0..n_threads {
            let barrier = barrier.clone();
            let total_num_operations = total_num_operations.clone();
            let weights = weights.clone();
            let treeindex = treeindex.clone();

            handles.push(std::thread::spawn(move || {
                let dist = WeightedIndex::new(&weights).unwrap();
                let mut rng = thread_rng();
                let choices: Vec<usize> = (0..ops_per_thread).map(|_| dist.sample(&mut rng)).collect();
                let mut data: Vec<usize> = (0..ops_per_thread).collect();
                data.shuffle(&mut rng);
                let mut n_operations = 0;
                barrier.wait();
                for i in 0..ops_per_thread {
                    match choices[i] {
                        0 => {
                            let mut iter = treeindex.raw_iter_mut();
                            iter.insert(data[i].to_be_bytes(), data[i].to_be_bytes());
                            n_operations += 1;
                        }
                        1 => {
                            let _ = treeindex.remove(&data[i].to_be_bytes());
                            n_operations += 1;
                        }
                        2 => {
                            let _ = treeindex.lookup(&data[i].to_be_bytes(), |_| ());
                            n_operations += 1;
                        }
                        3 => {
                            let mut scanner = treeindex.raw_iter();
                            scanner.seek(&data[i].to_be_bytes());
                            let mut scanned = 0;
                            while let Some(_) = scanner.next() {
                                scanned += 1;
                                if scanned >= 100 {
                                    break;
                                }
                            }
                            n_operations += scanned;
                        }
                        4 => {
                            let mut scanner = treeindex.raw_iter();
                            scanner.seek_to_first();
                            let mut scanned = 0;
                            while let Some(_) = scanner.next() {
                                scanned += 1;
                            }
                            n_operations += scanned;
                        }
                        _ => {
                            unreachable!("not an option");
                        }
                    }
                }
                barrier.wait();
                total_num_operations.fetch_add(n_operations, Relaxed);
            }));
        }

        barrier.wait();
        println!("Started!");
        let start_time = Instant::now();
        barrier.wait();
        let end_time = Instant::now();

        for handle in handles {
            handle.join().unwrap();
        }

        println!("{:?} {}", end_time.saturating_duration_since(start_time), total_num_operations.load(Relaxed));
    }
}
