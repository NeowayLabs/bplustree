use std::fmt;
use bplustree::error;

const COOL_BIT: u64 =        0b0000000000000000000000000000000000000000000000000000000000000010;
const UNSWIZZLED_BIT: u64 =  0b0000000000000000000000000000000000000000000000000000000000000001;
const HOT_MASK: u64 =        0b1111111111111111111111111111111111111111111111111111111111111100;
const SIZE_CLASS_MASK: u64 = 0b0000000000000000000000000000000000000000000000000000000001111110;
const PAGE_ID_MASK: u64 =    0b1111111111111111111111111111111111111111111111111111111110000000;

#[derive(PartialEq, Eq, Hash, Copy, Clone, Default)]
#[repr(transparent)]
pub struct Pid(u64);

impl Pid {
    #[inline]
    pub fn new(page_id: u64, size_class: u8) -> Pid {
        let mut n = (page_id << 7) & PAGE_ID_MASK;
        n = n | (((size_class as u64) << 1) & SIZE_CLASS_MASK);
        n = n | UNSWIZZLED_BIT;
        Pid(n)
    }

    pub fn new_invalid(size_class: u8) -> Pid {
        let page_id = 2u64.pow(57) - 1;
        let mut n = (page_id << 7) & PAGE_ID_MASK;
        n = n | (((size_class as u64) << 1) & SIZE_CLASS_MASK);
        n = n | UNSWIZZLED_BIT;
        Pid(n)
    }

    #[inline]
    pub fn page_id(&self) -> u64 {
        (self.0 & PAGE_ID_MASK) >> 7
    }

    #[inline]
    pub fn size_class(&self) -> u8 {
        ((self.0 & SIZE_CLASS_MASK) >> 1) as u8
    }
}

impl fmt::Debug for Pid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Pid")
            .field(&self.page_id())
            .field(&self.size_class())
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub enum RefOrPid<T: 'static> {
    Ref(&'static T),
    Cool(&'static T),
    Pid(Pid)
}

pub union Swip<T: 'static> {
    r: &'static T,
    pid: Pid,
    raw: u64
}

impl<T> Copy for Swip<T> {}

impl<T> Clone for Swip<T> {
    fn clone(&self) -> Swip<T> {
        *self
    }
}

impl<T: fmt::Debug> fmt::Debug for Swip<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.downcast() {
            RefOrPid::Ref(r) => {
                f.debug_struct("Swip")
                    .field("r", &r)
                    .finish()
            }
            RefOrPid::Cool(r) => {
                f.debug_struct("Swip")
                    .field("cool", &r)
                    .finish()
            }
            RefOrPid::Pid(pid) => {
                f.debug_struct("Swip")
                    .field("pid", &pid)
                    .finish()
            }
        }
    }
}

impl<T> PartialEq for Swip<T> {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.raw == other.raw }
    }
}

impl<T: 'static> Swip<T> {
    #[inline]
    pub fn from_ref(r: &'static T) -> Self {
        Swip { r }
    }

    #[inline]
    pub fn from_cool(r: &'static T) -> Self {
        Swip { raw: (r as *const T as u64) | COOL_BIT }
    }

    #[inline]
    pub fn from_pid(pid: Pid) -> Self {
        Swip { pid }
    }

    #[inline]
    pub fn downcast(&self) -> RefOrPid<T> {
        if unsafe { self.raw } & UNSWIZZLED_BIT == 1 {
            RefOrPid::Pid(unsafe { self.pid })
        } else if unsafe { self.raw } & COOL_BIT == 2 {
            RefOrPid::Cool(unsafe { &*((self.raw & HOT_MASK) as *const T) })
        } else {
            RefOrPid::Ref(unsafe { self.r })
        }
    }

    #[inline]
    pub fn try_downcast(&self) -> error::Result<RefOrPid<T>> {
        if self.invalid() {
            return Err(error::Error::Unwind);
        }

        Ok(self.downcast())
    }

    #[inline]
    pub fn as_ref(&self) -> &'static T {
        match self.downcast() {
            RefOrPid::Ref(r) => r,
            _ => {
                panic!("as_ref on unswizzled or cool swip");
            }
        }
    }

    #[inline]
    pub fn invalid(&self) -> bool {
        unsafe { self.raw == 0 }
    }

    #[inline]
    pub fn as_raw(&self) -> u64 {
        unsafe { self.raw }
    }

    #[inline]
    pub fn to_ref(&mut self, r: &'static T) {
        unsafe { self.r = r };
    }

    #[inline]
    pub fn to_cool(&mut self, r: &'static T) {
        unsafe { self.raw = (r as *const T as u64) | COOL_BIT };
    }

    #[inline]
    pub fn to_pid(&mut self, pid: Pid) {
        unsafe { self.pid = pid };
    }

    pub fn is_ref(&self) -> bool {
        !self.invalid() && unsafe { self.raw & (UNSWIZZLED_BIT | COOL_BIT) == 0 }
    }

    pub fn is_cool(&self) -> bool {
        unsafe { self.raw & (UNSWIZZLED_BIT | COOL_BIT) == 2 }
    }

    pub fn is_pid(&self) -> bool {
        unsafe { self.raw & (UNSWIZZLED_BIT | COOL_BIT) == 1 }
    }

//     #[inline]
//     pub fn swizzled(&self) -> bool {
//         !self.invalid() && (unsafe { self.raw } & UNSWIZZLED_BIT == 0u64)
//     }
}




#[cfg(test)]
mod tests {
    use super::{Pid, UNSWIZZLED_BIT, Swip, RefOrPid};

    #[test]
    fn pid_works() {
        let page_id_max = 2u64.pow(57) - 1;
        let size_class_max = 2u8.pow(6) - 1;

        let pid = Pid::new(page_id_max, size_class_max);
        assert_eq!(pid.page_id(), page_id_max);
        assert_eq!(pid.size_class(), size_class_max);

        assert_eq!(pid.0 & UNSWIZZLED_BIT, 1);
    }

    #[test]
    fn pid_swip_works() {
        let page_id_max = 2u64.pow(57) - 1;
        let size_class_max = 2u8.pow(6) - 1;

        let pid = Pid::new(page_id_max, size_class_max);

        let swip = Swip::<()>::from_pid(pid);

        if let RefOrPid::Pid(same_pid) = swip.downcast() {
            assert_eq!(pid, same_pid)
        } else {
            unreachable!("not a ref");
        }
    }

    #[test]
    fn ref_swip_works() {
        let x = Box::new(41);
        let static_ref: &'static usize = Box::leak(x);

        let swip = Swip::from_ref(static_ref);

        if let RefOrPid::Ref(r) = swip.downcast() {
            assert_eq!(*static_ref, *r);
        } else {
            unreachable!("not a pid");
        }
    }
}
