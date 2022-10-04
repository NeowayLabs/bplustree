#[cfg(test)]
mod benchmark {
    use crate::GenericBPlusTree;
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use serial_test::serial;

    #[derive(Clone)]
    struct Workload {
        size: usize,
        insert_local: usize,
        insert_remote: usize,
        scan: usize,
        read_local: usize,
        read_remote: usize,
        remove_local: usize,
        remove_remote: usize,
    }

    impl Workload {
        pub fn max_per_op_size(&self) -> usize {
            self.insert_local.max(
                self.insert_remote.max(
                    self.read_local.max(
                        self.read_remote
                            .max(self.remove_local.max(self.remove_remote)),
                    ),
                ),
            )
        }
        pub fn has_remote_op(&self) -> bool {
            self.insert_remote > 0 || self.read_remote > 0 || self.remove_remote > 0
        }
    }

    trait BenchmarkOperation<
        K: Clone + Eq + Hash + Ord + Send + Sync + std::fmt::Debug,
        V: Clone + Send + Sync + Unpin,
        H: BuildHasher
    >
    {
        fn insert_test(&self, k: K, v: V) -> bool;
        fn read_test(&self, k: &K) -> bool;
        fn scan_test(&self) -> usize;
        fn remove_test(&self, k: &K) -> bool;
    }

    impl<
            K: Clone + Eq + Hash + Ord + Send + Sync + std::fmt::Debug,
            V: Clone + Send + Sync + Unpin,
            H: BuildHasher,
            const IC: usize,
            const LC: usize
        > BenchmarkOperation<K, V, H> for GenericBPlusTree<K, V, IC, LC>
    {
        #[inline(always)]
        fn insert_test(&self, k: K, v: V) -> bool {
            let mut iter = self.raw_iter_mut();
            iter.insert(k, v);
            true
        }
        #[inline(always)]
        fn read_test(&self, k: &K) -> bool {
            self.lookup(k, |_| ()).is_some()
        }
        #[inline(always)]
        fn scan_test(&self) -> usize {
            let mut scanner = self.raw_iter();
            scanner.seek_to_first();
            let mut scanned = 0;
            while let Some(_) = scanner.next() {
                scanned += 1;
            }
            scanned
        }
        #[inline(always)]
        fn remove_test(&self, k: &K) -> bool {
            self.remove(k).is_some()
        }
    }

    trait ConvertFromUsize {
        fn convert(from: usize) -> Self;
    }

    impl ConvertFromUsize for usize {
        #[inline(always)]
        fn convert(from: usize) -> usize {
            from
        }
    }

    impl ConvertFromUsize for String {
        #[inline(always)]
        fn convert(from: usize) -> String {
            String::from(from.to_string())
        }
    }

    fn perform<
        K: Clone + ConvertFromUsize + Eq + Hash + Ord + Send + Sync + std::fmt::Debug,
        V: Clone + ConvertFromUsize + Send + Sync + Unpin,
        C: BenchmarkOperation<K, V, RandomState> + 'static + Send + Sync,
    >(
        num_threads: usize,
        start_index: usize,
        container: Arc<C>,
        workload: Workload,
    ) -> (Duration, usize) {
        use rand::thread_rng;
        use rand::seq::SliceRandom;

        let mut shuffled: Vec<_> = (0..30000000usize).collect();
        shuffled.shuffle(&mut thread_rng());
        let shuffled = Arc::new(shuffled);

        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let total_num_operations = Arc::new(AtomicUsize::new(0));
        let mut thread_handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let container_copied = container.clone();
            let barrier_copied = barrier.clone();
            let total_num_operations_copied = total_num_operations.clone();
            let workload_copied = workload.clone();
            let _shuffled = shuffled.clone();
            thread_handles.push(thread::spawn(move || {
                let mut num_operations = 0;
                let per_op_workload_size = workload_copied.max_per_op_size();
                let per_thread_workload_size = workload_copied.size * per_op_workload_size;
                barrier_copied.wait();
                for _ in 0..workload_copied.scan {
                    num_operations += container_copied.scan_test();
                }
                for i in 0..per_thread_workload_size {
                    let remote_thread_id = if num_threads < 2 {
                        0
                    } else {
                        (thread_id + 1 + i % (num_threads - 1)) % num_threads
                    };
                    assert!(num_threads < 2 || thread_id != remote_thread_id);
                    for j in 0..workload_copied.insert_local {
                        let local_index = thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result =
                            container_copied.insert_test(K::convert(local_index), V::convert(i));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.insert_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        container_copied.insert_test(K::convert(remote_index), V::convert(i));
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.read_local {
                        let local_index = thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = container_copied.read_test(&K::convert(local_index));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.read_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        container_copied.read_test(&K::convert(remote_index));
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.remove_local {
                        let local_index = thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        let result = container_copied.remove_test(&K::convert(local_index));
                        assert!(result || workload_copied.has_remote_op());
                        num_operations += 1;
                    }
                    for j in 0..workload_copied.remove_remote {
                        let remote_index = remote_thread_id * per_thread_workload_size
                            + i * per_op_workload_size
                            + j
                            + start_index;
                        container_copied.remove_test(&K::convert(remote_index));
                        num_operations += 1;
                    }
                }
                barrier_copied.wait();
                total_num_operations_copied.fetch_add(num_operations, Relaxed);
            }));
        }
        barrier.wait();
        let start_time = Instant::now();
        barrier.wait();
        let end_time = Instant::now();
        for handle in thread_handles {
            handle.join().unwrap();
        }
        (
            end_time.saturating_duration_since(start_time),
            total_num_operations.load(Relaxed),
        )
    }

    #[test]
    #[serial]
    fn bplustree_benchmark() {
        let num_threads_vector = vec![4];
        for num_threads in num_threads_vector {
            let treeindex: Arc<GenericBPlusTree<usize, usize, 128, 256>> = Arc::new(GenericBPlusTree::new());
            let workload_size = 262144;

            // 1. insert-local
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), insert.clone());
            println!(
                "treeindex-insert-local: {}, {:?}, {}, depth = {}",
                num_threads,
                duration,
                total_num_operations,
                treeindex.height()
            );

            // 2. scan
            let scan = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 1,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), scan.clone());
            println!(
                "treeindex-scan: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 3. read-local
            let read = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 1,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), read.clone());
            println!(
                "treeindex-read-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );

            // 4. remove-local
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), remove.clone());
            println!(
                "treeindex-remove-local: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
            assert_eq!(treeindex.len(), 0);

            // 5. insert-local-remote
            let insert = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 0,
                remove_remote: 0,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), insert.clone());
            println!(
                "treeindex-insert-local-remote: {}, {:?}, {}, depth = {}",
                num_threads,
                duration,
                total_num_operations,
                treeindex.height()
            );

            // 6. mixed
            let mixed = Workload {
                size: workload_size,
                insert_local: 1,
                insert_remote: 1,
                scan: 0,
                read_local: 1,
                read_remote: 1,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) = perform(
                num_threads,
                treeindex.len(),
                treeindex.clone(),
                mixed.clone(),
            );
            println!(
                "treeindex-mixed: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );


            // 7. remove-local-remote
            let remove = Workload {
                size: workload_size,
                insert_local: 0,
                insert_remote: 0,
                scan: 0,
                read_local: 0,
                read_remote: 0,
                remove_local: 1,
                remove_remote: 1,
            };
            let (duration, total_num_operations) =
                perform(num_threads, 0, treeindex.clone(), remove.clone());
            println!(
                "treeindex-remove-local-remote: {}, {:?}, {}",
                num_threads, duration, total_num_operations
            );
        }
    }

    #[test]
    #[serial]
    fn weighted() {
        use rand::prelude::*;
        use rand::distributions::WeightedIndex;
        use rand::seq::SliceRandom;

        let empty = false;
        let n_ops = 8000000;
        let n_threads = 8;
        let ops_per_thread = n_ops / n_threads;
        let weights = [20.0, 20.0, 55.0, 5.0, 0.00];

        println!("{}", ops_per_thread);

        let barrier = Arc::new(Barrier::new(n_threads + 1));
        let total_num_operations = Arc::new(AtomicUsize::new(0));

        let treeindex: Arc<GenericBPlusTree<usize, usize, 128, 256>> = Arc::new(GenericBPlusTree::new());

        if !empty {
            let mut rng = thread_rng();
            let mut data: Vec<usize> = (0..ops_per_thread).collect();
            data.shuffle(&mut rng);

            for i in 0..ops_per_thread {
                let mut iter = treeindex.raw_iter_mut();
                iter.insert(data[i], data[i]);
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
                            iter.insert(data[i], data[i]);
                            n_operations += 1;
                        }
                        1 => {
                            let _ = treeindex.remove(&data[i]);
                            n_operations += 1;
                        }
                        2 => {
                            let _ = treeindex.lookup(&data[i], |_| ());
                            n_operations += 1;
                        }
                        3 => {
                            let mut scanner = treeindex.raw_iter();
                            scanner.seek(&data[i]);
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
        let start_time = Instant::now();
        barrier.wait();
        let end_time = Instant::now();

        for handle in handles {
            handle.join().unwrap();
        }

        println!("{:?} {}", end_time.saturating_duration_since(start_time), total_num_operations.load(Relaxed));
    }
}
