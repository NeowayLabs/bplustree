//! Implementation of a hybrid latch based on the LeanStore paper.
//!
//! The key difference from a standard `RwLock` is the ability of acquiring optimistic read access
//! without perfoming any writes to memory. This mode of access is called optimistic because it allows reads
//! to the underlying data even though writers may be able to acquire exclusive access without
//! being blocked and perform writes while optimistic access is still in place.
//!
//! Those reads would normaly result in undefined behavior, but can be made safe by correctly validating
//! each optimistic access before allowing any side effects to happen. The validation is performed through
//! the [`OptimisticGuard::recheck`] method that returns an [`error::Error::Unwind`] if any writes could have taken place since
//! the acquisition of the optimistic access.
//!
//! We refer to unwinding as the premature return from a function that performed invalid accesses with the
//! error variant [`error::Error::Unwind`].
//!
//! The `?` operator is a very ergonomic way to perform this kind of validation
//! ```
//! use bplustree::latch::HybridLatch;
//! use bplustree::error;
//!
//! let latch = HybridLatch::new(10usize);
//! let mut guard = latch.optimistic_or_spin();
//!
//! loop {
//!     let access = || {
//!         let n = *guard;
//!         if n == 10 {
//!             guard.recheck()?; // validation
//!             println!("n is 10"); // side effect
//!         } else {
//!             guard.recheck()?; // validation
//!             println!("n is not 10"); // side effect
//!         }
//!
//!         error::Result::Ok(())
//!     };
//!
//!     match access() {
//!         Ok(_) => {
//!             break
//!         },
//!         Err(_) => {
//!             // Access was invalidated by some write from another thread,
//!             // acquire a new guard and retry
//!             guard = latch.optimistic_or_spin();
//!             continue
//!         }
//!     }
//! }
//! ```

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use parking_lot_core::SpinWait;
use std::{
    sync::atomic::{AtomicUsize, Ordering}
};
use std::cell::UnsafeCell;
use std::fmt;

use crate::error;

/// A hybrid latch that uses versioning to enable optimistic, shared or exclusive access to the
/// underlying data
pub struct HybridLatch<T: ?Sized> {
    version: AtomicUsize,
    lock: RwLock<()>,
    data: UnsafeCell<T>
}

unsafe impl<T: ?Sized + Send> Send for HybridLatch<T> {}
unsafe impl<T: ?Sized + Send + Sync> Sync for HybridLatch<T> {}

impl<T> HybridLatch<T> {
    /// Creates a new instance of a `HybridLatch<T>` which is unlocked.
    #[inline]
    pub fn new(data: T) -> HybridLatch<T> {
        HybridLatch {
            version: AtomicUsize::new(0),
            data: UnsafeCell::new(data),
            lock: RwLock::new(()),
        }
    }
}


impl<T: ?Sized> HybridLatch<T> {
    /// Locks this `HybridLatch` with exclusive write access, blocking the thread until it can be
    /// acquired.
    ///
    /// Returns an RAII guard which will release the exclusive access when dropped
    #[inline]
    pub fn exclusive(&self) -> ExclusiveGuard<'_, T> {
        let guard = self.lock.write();
        let version = self.version.load(Ordering::Relaxed) + 1;
        self.version.store(version, Ordering::Release);
        ExclusiveGuard {
            latch: self,
            guard: Some(guard),
            data: self.data.get(),
            version
        }
    }

    /// Attempts to acquire this `HybridLatch` with exclusive write access.
    ///
    /// If successful it returns an RAII guard which will release the exclusive access when dropped
    ///
    /// This function does not block, if the lock cannot be acquired it returns `None` instead.
    #[inline]
    pub fn try_exclusive(&self) -> Option<ExclusiveGuard<'_, T>> {
        let guard = self.lock.try_write()?;
        let version = self.version.load(Ordering::Relaxed) + 1;
        self.version.store(version, Ordering::Release);
        Some(ExclusiveGuard {
            latch: self,
            guard: Some(guard),
            data: self.data.get(),
            version
        })
    }

    #[inline]
    pub fn is_exclusively_latched(&self) -> bool {
        (self.version.load(Ordering::Acquire) & 1) == 1
    }

    #[inline]
    pub fn data_ptr(&self) -> *mut T {
        self.data.get()
    }

    #[inline]
    pub fn version(&self) -> usize {
        self.version.load(Ordering::Acquire)
    }

    /// Locks this `HybridLatch` with shared read access, blocking the thread until it can be
    /// acquired.
    ///
    /// Returns an RAII guard which will release the shared access when dropped
    #[inline]
    pub fn shared(&self) -> SharedGuard<'_, T> {
        let guard = self.lock.read();
        let version = self.version.load(Ordering::Relaxed);
        SharedGuard {
            latch: self,
            guard: Some(guard),
            data: self.data.get(),
            version
        }
    }

    /// Attempts to acquire this `HybridLatch` with shared read access.
    ///
    /// If successful it returns an RAII guard which will release the shared access when dropped
    ///
    /// This function does not block, if the lock cannot be acquired it returns `None` instead.
    #[inline]
    pub fn try_shared(&self) -> Option<SharedGuard<'_, T>> {
        let guard = self.lock.try_read()?;
        let version = self.version.load(Ordering::Relaxed);
        Some(SharedGuard {
            latch: self,
            guard: Some(guard),
            data: self.data.get(),
            version
        })
    }

    /// Acquires optimistic read access from this `HybridLatch`, spinning until it can be acquired.
    ///
    /// Optimistic access must be validated before performing any action based on a read of the
    /// underlying data. See [`OptimisticGuard::recheck`] for the details.
    ///
    /// Returns an RAII guard which will NOT validate any accesses when dropped.
    #[inline]
    pub fn optimistic_or_spin(&self) -> OptimisticGuard<'_, T> {
        let mut version = self.version.load(Ordering::Acquire);
        if (version & 1) == 1 {
            let mut spinwait = SpinWait::new();
            loop {
                version = self.version.load(Ordering::Acquire);
                if (version & 1) == 1 {
                    let result = spinwait.spin();
                    if !result {
                        spinwait.reset();
                    }
                    continue
                } else {
                    break
                }
            }
        }

        OptimisticGuard {
            latch: self,
            data: self.data.get(),
            version
        }
    }

    /// Tries to acquire optimistic read access from this `HybridLatch`, unwinding on contention.
    ///
    /// Optimistic access must be validated before performing any action based on a read of the
    /// underlying data. See [`OptimisticGuard::recheck`] for the details.
    ///
    /// Returns an RAII guard which will NOT validate any accesses when dropped.
    #[inline]
    pub fn optimistic_or_unwind(&self) -> error::Result<OptimisticGuard<'_, T>> {
        let version = self.version.load(Ordering::Acquire);
        if (version & 1) == 1 {
            return Err(error::Error::Unwind)
        }

        Ok(OptimisticGuard {
            latch: self,
            data: self.data.get(),
            version
        })
    }

    /// Tries to acquire optimistic read access from this `HybridLatch`, falling back to shared
    /// access on contention.
    ///
    /// Optimistic access must be validated before performing any action based on a read of the
    /// underlying data. See [`OptimisticGuard::recheck`] for the details.
    ///
    /// Acquiring shared access may block the current thread. Reads from shared access do not
    /// need to be validated.
    ///
    /// Returns either an [`OptimisticGuard`] or a [`SharedGuard`] through the [`OptimisticOrShared`] enum.
    #[inline]
    pub fn optimistic_or_shared(&self) -> OptimisticOrShared<'_, T> {
        let version = self.version.load(Ordering::Acquire);
        if (version & 1) == 1 {
            let guard = self.lock.read();
            let version = self.version.load(Ordering::Relaxed);
            OptimisticOrShared::Shared(SharedGuard {
                latch: self,
                guard: Some(guard),
                data: self.data.get(),
                version
            })
        } else {
            OptimisticOrShared::Optimistic(OptimisticGuard {
                latch: self,
                data: self.data.get(),
                version
            })
        }
    }

    /// Tries to acquire optimistic read access from this `HybridLatch`, falling back to exclusive
    /// access on contention.
    ///
    /// Optimistic access must be validated before performing any action based on a read of the
    /// underlying data. See [`OptimisticGuard::recheck`] for the details.
    ///
    /// Acquiring exclusive access may block the current thread. Reads or writes from exclusive access do not
    /// need to be validated.
    ///
    /// Returns either an [`OptimisticGuard`] or an [`ExclusiveGuard`] through the [`OptimisticOrExclusive`] enum.
    #[inline]
    pub fn optimistic_or_exclusive(&self) -> OptimisticOrExclusive<'_, T> {
        let version = self.version.load(Ordering::Acquire);
        if (version & 1) == 1 {
            let guard = self.lock.write();
            let version = self.version.load(Ordering::Relaxed) + 1;
            self.version.store(version, Ordering::Release);
            OptimisticOrExclusive::Exclusive(ExclusiveGuard {
                latch: self,
                guard: Some(guard),
                data: self.data.get(),
                version
            })
        } else {
            OptimisticOrExclusive::Optimistic(OptimisticGuard {
                latch: self,
                data: self.data.get(),
                version
            })
        }
    }
}

impl<T: ?Sized> std::convert::AsMut<T> for HybridLatch<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.get() }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for HybridLatch<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.try_shared() {
            Some(guard) => f.debug_struct("HybridLatch").field("data", &&*guard).finish(),
            None => {
                struct LockedPlaceholder;
                impl fmt::Debug for LockedPlaceholder {
                    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("<locked>")
                    }
                }

                f.debug_struct("HybridLatch")
                    .field("data", &LockedPlaceholder)
                    .finish()
            }
        }
    }
}

/// Trait to allow using any guard when only read access is needed.
pub trait HybridGuard<T: ?Sized, P: ?Sized = T> {
    /// Allows read access to the undelying data, which must be validated before any side effects
    fn inner(&self) -> &T;

    /// Allows read access to the unmapped data, which must be validated before any side effects
    fn as_unmapped(&self) -> &P;

    /// Validates any accesses performed.
    ///
    /// The user of a `HybridGuard` must validate all accesses because there is no guarantee of which
    /// mode the accesses are being performed.
    ///
    /// If validation fails it returns [`error::Error::Unwind`].
    fn recheck(&self) -> error::Result<()>;

    /// Returns a reference to the original `HybridLatch` struct
    fn latch(&self) -> &HybridLatch<P>;
}

/// Structure used to perform optimistic accesses and validation.
pub struct OptimisticGuard<'a, T: ?Sized, P: ?Sized = T> {
    latch: &'a HybridLatch<P>,
    data: *const T,
    version: usize
}

unsafe impl<'a, T: ?Sized + Sync, P: ?Sized + Sync> Sync for OptimisticGuard<'a, T, P> {}

impl<'a, T: ?Sized, P: ?Sized> OptimisticGuard<'a, T, P> {
    /// Validates all previous optimistic accesses since the creation of the guard,
    /// if validation fails an [`error::Error::Unwind`] is returned to signal that the
    /// stack should be unwinded (by conditional returns) to a safe state.
    #[inline]
    pub fn recheck(&self) -> error::Result<()> {
        debug_assert!((self.version & 1) == 0);
        if self.version != self.latch.version.load(Ordering::Acquire) {
            return Err(error::Error::Unwind)
        }
        Ok(())
    }

    /// Tries to acquire exclusive access after validation of all previous optimistic accesses on
    /// this guard.
    ///
    /// If validation fails it returns [`error::Error::Unwind`].
    #[inline]
    pub fn to_exclusive(self) -> error::Result<ExclusiveGuard<'a, T, P>> {
        let new_version = self.version + 1;
        let expected = self.version;
        let locked = self.latch.lock.write();
        if let Err(_v) = self.latch.version
            .compare_exchange(
                expected,
                new_version,
                Ordering::Acquire,
                Ordering::Acquire)
        {
            drop(locked);
            return Err(error::Error::Unwind)
        }

        Ok(ExclusiveGuard {
            latch: self.latch,
            guard: Some(locked),
            data: self.data as *mut _,
            version: new_version
        })
    }

    /// Attempts to acquire exclusive access failing if already latched of if the validation of all previous optimistic accesses on
    /// this guard is not successful.
    ///
    /// If validation fails it returns [`error::Error::Unwind`].
    #[inline]
    pub fn try_to_exclusive(self) -> error::Result<ExclusiveGuard<'a, T, P>> {
        let new_version = self.version + 1;
        let expected = self.version;
        if let Some(locked) = self.latch.lock.try_write() {
            if let Err(_v) = self.latch.version
                .compare_exchange(
                    expected,
                    new_version,
                    Ordering::Acquire,
                    Ordering::Acquire)
                {
                    drop(locked);
                    return Err(error::Error::Unwind)
                }

            Ok(ExclusiveGuard {
                latch: self.latch,
                guard: Some(locked),
                data: self.data as *mut _,
                version: new_version
            })
        } else {
            Err(error::Error::Unwind)
        }
    }

    /// Tries to acquire shared access after validation of all previous optimistic accesses on
    /// this guard.
    ///
    /// If validation fails it returns [`error::Error::Unwind`].
    #[inline]
    pub fn to_shared(self) -> error::Result<SharedGuard<'a, T, P>> {
        if let Some(guard) = self.latch.lock.try_read() {
            if self.version != self.latch.version.load(Ordering::Relaxed) {
                return Err(error::Error::Unwind)
            }

            Ok(SharedGuard {
                latch: self.latch,
                guard: Some(guard),
                data: self.data,
                version: self.version
            })
        } else {
            return Err(error::Error::Unwind)
        }
    }

    /// Returns a reference to the original `HybridLatch` struct
    pub fn latch(&self) -> &'a HybridLatch<P> {
        self.latch
    }

    pub fn map<U: ?Sized, F>(s: Self, f: F) -> error::Result<OptimisticGuard<'a, U, P>>
    where
        F: FnOnce(&T) -> error::Result<&U>
    {
        let latch = s.latch;
        let version = s.version;
        let data = f(unsafe { &*s.data })?;
        std::mem::forget(s);
        Ok(OptimisticGuard {
            latch,
            version,
            data
        })
    }

    pub fn unmap(s: Self) -> OptimisticGuard<'a, P, P> {
        let latch = s.latch;
        let version = s.version;
        let data = latch.data.get();
        std::mem::forget(s);
        OptimisticGuard {
            latch,
            version,
            data
        }
    }
}

impl<'a, T: ?Sized, P: ?Sized> std::ops::Deref for OptimisticGuard<'a, T, P> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.data }
    }
}

impl<'a, T: ?Sized, P: ?Sized> HybridGuard<T, P> for OptimisticGuard<'a, T, P> {
    fn inner(&self) -> &T {
        self
    }
    fn as_unmapped(&self) -> &P {
        unsafe { &*self.latch().data.get() }
    }
    fn recheck(&self) -> error::Result<()> {
        self.recheck()
    }
    fn latch(&self) -> &HybridLatch<P> {
        self.latch()
    }
}

/// RAII structure used to release the exclusive write access of a latch when dropped.
pub struct ExclusiveGuard<'a, T: ?Sized, P: ?Sized = T> {
    latch: &'a HybridLatch<P>,
    #[allow(dead_code)]
    guard: Option<RwLockWriteGuard<'a, ()>>,
    data: *mut T,
    version: usize
}

unsafe impl<'a, T: ?Sized + Sync, P: ?Sized + Sync> Sync for ExclusiveGuard<'a, T, P> {}

impl<'a, T: ?Sized, P: ?Sized> ExclusiveGuard<'a, T, P> {
    /// A sanity assertion, exclusive guards do not need to be validated
    #[inline]
    pub fn recheck(&self) {
        assert!(self.version == self.latch.version.load(Ordering::Relaxed));
    }

    /// Unlocks the `HybridLatch` returning a [`OptimisticGuard`] in the current version
    #[inline]
    pub fn unlock(self) -> OptimisticGuard<'a, T, P> {
        let new_version = self.version + 1;
        let latch = self.latch;
        let data = self.data;
        // The version is incremented in drop
        drop(self);
        OptimisticGuard {
            latch,
            data,
            version: new_version
        }
    }

    #[inline]
    pub fn downgrade(mut self) -> SharedGuard<'a, T, P> {
        let guard = self.guard.take().expect("must exist");
        let new_version = self.version + 1;
        let latch = self.latch;
        let data = self.data;
        // The version is incremented in drop
        drop(self);
        let shared_guard = RwLockWriteGuard::downgrade(guard);
        SharedGuard {
            latch,
            guard: Some(shared_guard),
            data,
            version: new_version
        }
    }

    /// Returns a reference to the original `HybridLatch` struct
    pub fn latch(&self) -> &'a HybridLatch<P> {
        self.latch
    }

    pub fn as_unmapped_mut(&mut self) -> &mut P {
        unsafe { &mut *self.latch().data.get() }
    }

    pub fn map<U: ?Sized, F>(mut s: Self, f: F) -> ExclusiveGuard<'a, U, P>
    where
        F: FnOnce(&mut T) -> &mut U
    {
        let latch = s.latch;
        let version = s.version;
        let guard = s.guard.take();
        let data = f(unsafe { &mut *s.data });
        std::mem::forget(s);
        ExclusiveGuard {
            latch,
            version,
            guard,
            data
        }
    }

    pub fn unmap(mut s: Self) -> ExclusiveGuard<'a, P, P> {
        let latch = s.latch;
        let version = s.version;
        let guard = s.guard.take();
        let data = latch.data.get();
        std::mem::forget(s);
        ExclusiveGuard {
            latch,
            version,
            guard,
            data
        }
    }
}

impl<'a, T: ?Sized, P: ?Sized> Drop for ExclusiveGuard<'a, T, P> {
    #[inline]
    fn drop(&mut self) {
        let new_version = self.version + 1;
        self.latch.version.store(new_version, Ordering::Release);
    }
}

impl<'a, T: ?Sized, P: ?Sized> std::ops::Deref for ExclusiveGuard<'a, T, P> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.data }
    }
}

impl<'a, T: ?Sized, P: ?Sized> std::ops::DerefMut for ExclusiveGuard<'a, T, P> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data }
    }
}

impl<'a, T: ?Sized, P: ?Sized> std::convert::AsMut<T> for ExclusiveGuard<'a, T, P> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data }
    }
}

impl<'a, T: ?Sized + fmt::Debug, P: ?Sized> fmt::Debug for ExclusiveGuard<'a, T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<'a, T: ?Sized, P: ?Sized> HybridGuard<T, P> for ExclusiveGuard<'a, T, P> {
    fn inner(&self) -> &T {
        self
    }
    fn as_unmapped(&self) -> &P {
        unsafe { &*self.latch().data.get() }
    }
    fn recheck(&self) -> error::Result<()> {
        self.recheck();
        Ok(())
    }
    fn latch(&self) -> &HybridLatch<P> {
        self.latch()
    }
}

/// RAII structure used to release the shared read access of a latch when dropped.
pub struct SharedGuard<'a, T: ?Sized, P: ?Sized = T> {
    latch: &'a HybridLatch<P>,
    #[allow(dead_code)]
    guard: Option<RwLockReadGuard<'a, ()>>,
    data: *const T,
    version: usize
}

unsafe impl<'a, T: ?Sized + Sync, P: ?Sized + Sync> Sync for SharedGuard<'a, T, P> {}

impl<'a, T: ?Sized, P: ?Sized> SharedGuard<'a, T, P> {
    /// A sanity assertion, exclusive guards do not need to be validated
    #[inline]
    pub fn recheck(&self) {
        assert!(self.version == self.latch.version.load(Ordering::Relaxed));
    }

    /// Unlocks the `HybridLatch` returning a [`OptimisticGuard`] in the current version
    #[inline]
    pub fn unlock(self) -> OptimisticGuard<'a, T, P> {
        OptimisticGuard {
            latch: self.latch,
            data: self.data,
            version: self.version
        }
    }

    /// Returns a [`OptimisticGuard`] in the current version without consuming the original `SharedGuard`
    #[inline]
    pub fn as_optimistic<'b>(&'b self) -> OptimisticGuard<'b, T, P> {
        OptimisticGuard {
            latch: self.latch,
            data: self.data,
            version: self.version
        }
    }

    /// Returns a reference to the original `HybridLatch` struct
    pub fn latch(&self) -> &'a HybridLatch<P> {
        self.latch
    }

    pub fn map<U: ?Sized, F>(mut s: Self, f: F) -> SharedGuard<'a, U, P>
    where
        F: FnOnce(&T) -> &U
    {
        let latch = s.latch;
        let version = s.version;
        let guard = s.guard.take();
        let data = f(unsafe { &*s.data });
        std::mem::forget(s);
        SharedGuard {
            latch,
            version,
            guard,
            data
        }
    }

    pub fn unmap(mut s: Self) -> SharedGuard<'a, P, P> {
        let latch = s.latch;
        let version = s.version;
        let guard = s.guard.take();
        let data = latch.data.get();
        std::mem::forget(s);
        SharedGuard {
            latch,
            version,
            guard,
            data
        }
    }
}

impl<'a, T: ?Sized, P: ?Sized> std::ops::Deref for SharedGuard<'a, T, P> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.data }
    }
}

impl<'a, T: ?Sized + fmt::Debug, P: ?Sized> fmt::Debug for SharedGuard<'a, T, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<'a, T: ?Sized, P: ?Sized> HybridGuard<T, P> for SharedGuard<'a, T, P> {
    fn inner(&self) -> &T {
        self
    }
    fn as_unmapped(&self) -> &P {
        unsafe { &*self.latch().data.get() }
    }
    fn recheck(&self) -> error::Result<()> {
        self.recheck();
        Ok(())
    }
    fn latch(&self) -> &HybridLatch<P> {
        self.latch()
    }
}

/// Either an `OptimisticGuard` or a `SharedGuard`.
pub enum OptimisticOrShared<'a, T: ?Sized, P: ?Sized = T> {
    Optimistic(OptimisticGuard<'a, T, P>),
    Shared(SharedGuard<'a, T, P>)
}

impl<'a, T: ?Sized, P: ?Sized> OptimisticOrShared<'a, T, P> {
    #[inline]
    pub fn recheck(&self) -> error::Result<()> {
        match self {
            OptimisticOrShared::Optimistic(g) => g.recheck(),
            OptimisticOrShared::Shared(g) => {
                g.recheck();
                Ok(())
            }
        }
    }
}

/// Either an `OptimisticGuard` or an `ExclusiveGuard`.
pub enum OptimisticOrExclusive<'a, T: ?Sized, P: ?Sized = T> {
    Optimistic(OptimisticGuard<'a, T, P>),
    Exclusive(ExclusiveGuard<'a, T, P>)
}

impl<'a, T: ?Sized, P: ?Sized> OptimisticOrExclusive<'a, T, P> {
    #[inline]
    pub fn recheck(&self) -> error::Result<()> {
        match self {
            OptimisticOrExclusive::Optimistic(g) => g.recheck(),
            OptimisticOrExclusive::Exclusive(g) => {
                g.recheck();
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HybridLatch;
    use crate::error;
    use std::cell::UnsafeCell;
    use std::sync::Arc;
    use std::thread;
    use serial_test::serial;

    struct Wrapper<T>(UnsafeCell<[T; 1000]>);
    unsafe impl<T: Send> Send for Wrapper<T> {}
    unsafe impl<T: Send + Sync> Sync for Wrapper<T> {}

    #[test]
    #[serial]
    fn single_threaded_reader_baseline() {
        let data = [1usize; 1000];
        let mut result = 1usize;
        let t0 = std::time::Instant::now();
        for _i in 0..4000000 {
            for j in 0..1000 {
                result = result.saturating_mul(data[j]);
            }
        }
        println!("Single threaded reader done in {:?}", t0.elapsed());
        assert!(result == 1);
    }

    #[test]
    #[serial]
    fn concurrent_reading_and_writing() {
        let data = Arc::new(Wrapper(UnsafeCell::new([1usize; 1000])));
        let latch = Arc::new(HybridLatch::new(()));

        let n_readers = 3;
        let n_writers = 1;
        let n = n_readers + n_writers;
        let barrier = Arc::new(std::sync::Barrier::new(n + 1));

        // unsafe { (*data.0.get())[3] = 2 };

        let mut readers = vec![];
        for _i in 0..n_readers {
            let data = data.clone();
            let latch = latch.clone();
            let barrier = barrier.clone();

            let handle = thread::spawn(move || {
                barrier.wait();
                let mut result = 1usize;
                for _i in 0..4000000 {
                    loop {
                        let res = {
                            let attempt = || {
                                let locked = latch.optimistic_or_spin();
                                let arr = data.0.get();
                                let mut result = 1usize;
                                for j in 0..1000 {
                                    result = result.saturating_mul(unsafe { (*arr)[j] });
                                }
                                locked.recheck()?;
                                error::Result::Ok(result)
                            };
                            attempt()
                        };
                        match res {
                            Ok(v) => {
                                result *= v;
                                break;
                            }
                            Err(_) => {
                                // TODO maybe backoff;
                                continue;
                            }
                        }
                    }
                    assert!(result == 1);
                }

                assert!(result == 1);
            });
            readers.push(handle);
        }

        let mut writers = vec![];
        for _i in 0..n_writers {
            let data = data.clone();
            let latch = latch.clone();
            let barrier = barrier.clone();

            let handle = thread::spawn(move || {
                barrier.wait();
                let seconds = 10f64;
                let micros_per_sec = 1_000_000;
                let freq = 100;
                let critical = 1000;
                for _i in 0..(seconds * freq as f64) as usize {
                    thread::sleep(std::time::Duration::from_micros((micros_per_sec / freq) - critical));
                    {
                        let _locked = latch.exclusive();
                        unsafe { (*data.0.get())[3] = 2 };
                        thread::sleep(std::time::Duration::from_micros(critical));
                        unsafe { (*data.0.get())[3] = 1 };
                    }
                }
            });
            writers.push(handle);
        }

        barrier.wait();
        let t0 = std::time::Instant::now();

        for handle in readers {
            handle.join().unwrap();
        }

        println!("Readers done in {:?}", t0.elapsed());

        for handle in writers {
            handle.join().unwrap();
        }

        println!("Writers done in at most {:?}", t0.elapsed());
    }

    #[test]
    #[serial]
    fn single_threaded_option_reader_baseline() {
        let data = [Some(1usize); 1000];
        let mut result = 1usize;
        let t0 = std::time::Instant::now();
        for _i in 0..4000000 {
            for j in 0..1000 {
                let opt = &data[j];
                if let Some(n) = opt {
                    result = result.saturating_mul(*n);
                } else {
                    result = 0;
                }
            }
        }
        println!("Single threaded option reader done in {:?}", t0.elapsed());
        assert!(result == 1);
    }

    #[test]
    #[serial]
    fn concurrent_option_reading_and_writing() {
        let data = Arc::new(Wrapper(UnsafeCell::new([Some(1usize); 1000])));
        let latch = Arc::new(HybridLatch::new(()));

        let n_readers = 3;
        let n_writers = 1;
        let n = n_readers + n_writers;
        let barrier = Arc::new(std::sync::Barrier::new(n + 1));

        // unsafe { (*data.0.get())[3] = 2 };

        let mut readers = vec![];
        for _i in 0..n_readers {
            let data = data.clone();
            let latch = latch.clone();
            let barrier = barrier.clone();

            let handle = thread::spawn(move || {
                barrier.wait();
                let mut result = 1usize;
                for _i in 0..4000000 {
                    loop {
                        let res = {
                            let attempt = || {
                                let locked = latch.optimistic_or_spin();
                                let arr = data.0.get();
                                let mut result = 1usize;
                                for j in 0..1000 {
                                    let opt = unsafe { &(*arr)[j] };
                                    if let Some(n) = opt {
                                        result = result.saturating_mul(*n);
                                    } else {
                                        result = 0;
                                    }
                                }
                                locked.recheck()?;
                                error::Result::Ok(result)
                            };
                            attempt()
                        };
                        match res {
                            Ok(v) => {
                                result *= v;
                                break;
                            }
                            Err(_) => {
                                // TODO maybe backoff;

                                continue;
                            }
                        }
                    }
                    assert!(result == 1);
                }

                assert!(result == 1);
            });
            readers.push(handle);
        }

        let mut writers = vec![];
        for _i in 0..n_writers {
            let data = data.clone();
            let latch = latch.clone();
            let barrier = barrier.clone();

            let handle = thread::spawn(move || {
                barrier.wait();
                let seconds = 10f64;
                let micros_per_sec = 1_000_000;
                let freq = 100;
                let critical = 1000;
                for _i in 0..(seconds * freq as f64) as usize {
                    thread::sleep(std::time::Duration::from_micros((micros_per_sec / freq) - critical));
                    {
                        let _locked = latch.exclusive();
                        unsafe { (*data.0.get())[3] = None };
                        thread::sleep(std::time::Duration::from_micros(critical));
                        unsafe { (*data.0.get())[3] = Some(1) };
                    }
                }
            });
            writers.push(handle);
        }

        barrier.wait();
        let t0 = std::time::Instant::now();

        for handle in readers {
            handle.join().unwrap();
        }

        println!("Readers done in {:?}", t0.elapsed());

        for handle in writers {
            handle.join().unwrap();
        }

        println!("Writers done in at most {:?}", t0.elapsed());
    }
}
