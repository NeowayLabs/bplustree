use crate::latch::{HybridLatch, OptimisticGuard, ExclusiveGuard, OptimisticOrExclusive};
use crate::error;
use super::BufferFrame;

pub(crate) trait BfLatchExt {
    fn exclusive_bf(&self) -> ExclusiveGuard<'_, BufferFrame>;
    fn optimistic_or_exclusive_bf(&self) -> OptimisticOrExclusive<'_, BufferFrame>;
}

pub(crate) trait BfOptimisticGuardExt<'a, T: ?Sized> {
    fn to_exclusive_bf(self) -> error::Result<ExclusiveGuard<'a, T, BufferFrame>>;
}

impl BfLatchExt for HybridLatch<BufferFrame> {
    #[inline]
    fn exclusive_bf(&self) -> ExclusiveGuard<'_, BufferFrame> {
        let mut guard = self.exclusive();
        // guard.dirty = true;
        guard.page.gsn += 1;
        guard
    }
    #[inline]
    fn optimistic_or_exclusive_bf(&self) -> OptimisticOrExclusive<'_, BufferFrame> {
        match self.optimistic_or_exclusive() {
            OptimisticOrExclusive::Exclusive(mut guard) => {
                // guard.dirty = true;
                guard.page.gsn += 1;
                OptimisticOrExclusive::Exclusive(guard)
            }
            guard => guard
        }
    }
}

impl<'a, T: ?Sized> BfOptimisticGuardExt<'a, T> for OptimisticGuard<'a, T, BufferFrame> {
    #[inline]
    fn to_exclusive_bf(self) -> error::Result<ExclusiveGuard<'a, T, BufferFrame>> {
        self.to_exclusive().map(|mut g| {
            // g.as_unmapped_mut().dirty = true;
            g.as_unmapped_mut().page.gsn += 1;
            g
        })
    }
}
