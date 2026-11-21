//! Global allocator: large blocks come straight from mmap with MADV_HUGEPAGE,
//! which cuts page-fault overhead for the multi-GB record buffers.
use std::alloc::{GlobalAlloc, Layout, System};

pub struct HugeAlloc;

const BIG: usize = 1 << 21; // 2 MiB

unsafe impl GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= 4096 {
            unsafe {
                let p = libc::mmap(
                    std::ptr::null_mut(),
                    l.size(),
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                    -1,
                    0,
                );
                if p == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                libc::madvise(p, l.size(), libc::MADV_HUGEPAGE);
                p as *mut u8
            }
        } else {
            unsafe { System.alloc(l) }
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= 4096 {
            unsafe { self.alloc(l) } // fresh anonymous mmap is zeroed
        } else {
            unsafe { System.alloc_zeroed(l) }
        }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if l.size() >= BIG && l.align() <= 4096 {
            unsafe {
                libc::munmap(p as *mut libc::c_void, l.size());
            }
        } else {
            unsafe { System.dealloc(p, l) }
        }
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let old = l.size();
        if old < BIG && new_size < BIG {
            return unsafe { System.realloc(p, l, new_size) };
        }
        if old >= BIG && new_size >= BIG && l.align() <= 4096 {
            unsafe {
                let q = libc::mremap(p as *mut libc::c_void, old, new_size, libc::MREMAP_MAYMOVE);
                if q == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                libc::madvise(q, new_size, libc::MADV_HUGEPAGE);
                return q as *mut u8;
            }
        }
        unsafe {
            let nl = Layout::from_size_align_unchecked(new_size, l.align());
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, old.min(new_size));
                self.dealloc(p, l);
            }
            q
        }
    }
}
