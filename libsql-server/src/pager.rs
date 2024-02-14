use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ffi::{c_int, c_void};
use std::mem::{align_of, size_of};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use hashbrown::HashMap;
use parking_lot::Mutex;
use rusqlite::ffi::{sqlite3_pcache, sqlite3_pcache_methods2, sqlite3_pcache_page};
use uuid::Uuid;

use crate::LIBSQL_PAGE_SIZE;

unsafe impl Send for Allocator {}
unsafe impl Sync for Allocator {}

struct Allocator {
    // points the first page in the free list
    // TODO: have multiple free lists if we notice too much contention
    free_list_head: parking_lot::Mutex<u32>,
    slab: Box<[UnsafeCell<Page>]>,
    num_pages: usize,
}

const FLAG_FREE: usize = 62;
const FLAG_PIN: usize = 63;

#[derive(Debug)]
#[repr(C)]
struct Page {
    p_page: *mut c_void,
    p_extra: *mut c_void,
    // bits 0..31: page pos if not free page, or next free page if free page
    // bit 63: pin
    // bit 62: free
    flags: u64,
    data: [u8; PAGER_PAGE_SIZE + PAGER_EXTRA_SIZE],
}

impl Page {
    fn free(&mut self, next: u32) {
        self.flags = (1 << FLAG_FREE) | next as u64;
        self.clear();
    }

    fn is_free(&self) -> bool {
        self.flags & 1 << FLAG_FREE != 0
    }

    fn is_pinned(&self) -> bool {
        self.flags & 1 << FLAG_PIN != 0
    }

    fn clear(&mut self) {
        self.data[PAGER_PAGE_SIZE..].fill(0);
    }

    fn pin(&mut self, key: u32) {
        tracing::trace!(key, "pin");
        self.flags = 1 << FLAG_PIN | key as u64;
    }

    fn unpin(&mut self) -> u32 {
        let key = self.flags as u32;
        // TODO: maybe not necessary to have a flag since key > 0
        // clean pin flag and pinned key
        self.flags &= !(1 << FLAG_PIN);
        assert!(!self.is_pinned());
        self.flags &= !(u32::MAX as u64);
        key
    }

    fn key(&self) -> u32 {
        self.flags as u32
    }

    fn next(&self) -> u32 {
        if !self.is_free() {
            panic!("tried to get next page, but page is not free");
        }

        self.flags as u32
    }

    fn allocate(&mut self, current: u32) {
        self.flags = current as u64;
    }
}

impl Allocator {
    fn new(page_size: usize, extra_size: usize, max_pages: usize) -> Self {
        assert_eq!(page_size, PAGER_PAGE_SIZE);
        assert_eq!(extra_size, PAGER_EXTRA_SIZE);
        // todo: round up to a multiple aligned to 8
        let size = max_pages * size_of::<Page>();
        let layout = Layout::from_size_align(size, align_of::<Page>()).unwrap();
        let mut slab: Box<[UnsafeCell<Page>]> = unsafe {
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                panic!("OOM");
            }
            let v: Vec<UnsafeCell<Page>> = Vec::from_raw_parts(ptr as *mut _, max_pages, max_pages);
            v.into()
        };

        // initialize the intrusive list: each page is initially linked to the next
        slab.iter_mut().enumerate().for_each(|(i, p)| {
            let page = p.get_mut();
            page.free((i + 1) as u32);
            page.p_page = page.data.as_mut_ptr() as *mut _;
            page.p_extra =
                unsafe { page.data.as_mut_ptr().offset(LIBSQL_PAGE_SIZE as _) as *mut _ };
            assert_eq!((page.p_page as usize) % 8, 0);
        });

        Self {
            free_list_head: 0.into(),
            slab,
            num_pages: max_pages,
        }
    }

    // alloc a page from the alloactor, returning a pointer to it, and the page
    fn alloc(&self) -> Option<&mut Page> {
        let mut current = self.free_list_head.lock();
        if (*current as usize) < self.num_pages {
            let page = &self.slab[*current as usize];
            unsafe {
                let page = page.get();
                let page: &mut Page = &mut *page;
                let next = page.next();
                page.allocate(*current);
                *current = next;
                Some(&mut *(page as *mut _))
            }
        } else {
            None
        }
    }

    fn offset_of(&self, p: &Page) -> u32 {
        (((p as *const _ as usize) - (self.slab.as_ptr() as usize)) / size_of::<Page>()) as u32
    }

    fn free(&self, page: &mut Page) {
        tracing::trace!(page = self.offset_of(page), "free");
        assert!(!page.is_free());
        let mut next = self.free_list_head.lock();
        let current = self.offset_of(page);
        page.free(*next);
        *next = current as u32;
    }
}

const PAGER_PAGE_SIZE: usize = 4096;
const PAGER_EXTRA_SIZE: usize = 224;

struct Pager {
    alloc: &'static Allocator,
    pages: Mutex<HashMap<u32, &'static mut Page>>,
    span: tracing::Span,
}

impl Drop for Pager {
    fn drop(&mut self) {
        // return pages to allocator
        for (_, page) in std::mem::take(&mut *self.pages.lock()).into_iter() {
            // make the list fist, and then bulk free.
            self.alloc.free(page);
        }
    }
}

static PAGER_CACHE: once_cell::sync::OnceCell<Allocator> = once_cell::sync::OnceCell::new();
pub static PAGER_CACHE_SIZE: AtomicUsize = AtomicUsize::new(0);

extern "C" fn init(_arg: *mut c_void) -> c_int {
    0
}

extern "C" fn create(
    page_size: c_int,
    extra_size: c_int,
    _purgeable: c_int,
) -> *mut sqlite3_pcache {
    let allocator = PAGER_CACHE.get_or_init(|| {
        Allocator::new(
            page_size as usize,
            extra_size as usize,
            PAGER_CACHE_SIZE.load(SeqCst),
        )
    });
    let span = tracing::span!(
        tracing::Level::INFO,
        "pager",
        uuid = Uuid::new_v4().to_string()
    );
    let pager = Pager {
        alloc: allocator,
        pages: HashMap::new().into(),
        span,
    };

    Box::into_raw(Box::new(pager)) as *mut Pager as *mut _
}

extern "C" fn cache_size(_cache: *mut sqlite3_pcache, _size: c_int) {}
extern "C" fn page_count(cache: *mut sqlite3_pcache) -> c_int {
    let cache = unsafe { &*(cache as *mut Pager) };
    cache.pages.lock().len() as _
}

extern "C" fn fetch(
    cache: *mut sqlite3_pcache,
    key: u32,
    create_flag: c_int,
) -> *mut sqlite3_pcache_page {
    let cache = unsafe { &*(cache as *mut Pager) };
    let _span = cache.span.enter();
    tracing::trace!(key = key, "fetch");
    let mut pages = cache.pages.lock();
    match pages.get_mut(&key) {
        Some(page) => {
            tracing::trace!(key = key, "found");
            page.pin(key);
            assert_eq!(page.key(), key);
            (*page) as *mut _ as *mut _
        }
        None => {
            // try to find an unpinned page
            match pages.extract_if(|_, p| !p.is_pinned()).next() {
                Some((_, page)) => {
                    tracing::trace!(key, page = cache.alloc.offset_of(page), "reuse");
                    page.clear();
                    page.pin(key);
                    let ptr = page as *mut _;
                    pages.insert(key, page);
                    ptr as *mut _
                }
                None if create_flag == 0 => std::ptr::null_mut(),
                None if create_flag != 0 => {
                    // try alloc one from global pool
                    match cache.alloc.alloc() {
                        Some(page) => {
                            tracing::trace!(key, page = cache.alloc.offset_of(page), "alloc");
                            page.pin(key);
                            assert_eq!(page.key(), key);
                            let ptr = page as *mut _;
                            assert!(pages.insert(key, page).is_none());
                            ptr as *mut _
                        }
                        None => std::ptr::null_mut(),
                    }
                }
                None => unreachable!(),
            }
        }
    }
}

extern "C" fn unpin(cache: *mut sqlite3_pcache, page: *mut sqlite3_pcache_page, discard: c_int) {
    let page: &mut Page = unsafe { &mut *(page as *mut Page) };
    let cache = unsafe { &mut *(cache as *mut Pager) };
    let _span = cache.span.enter();
    let pages = &mut cache.pages;

    let key = page.unpin();
    tracing::trace!(key, page = cache.alloc.offset_of(page), "unpin");

    if discard != 0 {
        let page = pages.lock().remove(&key).expect("missing page");
        cache.alloc.free(page);
    }
}

extern "C" fn rekey(
    cache: *mut sqlite3_pcache,
    data: *mut sqlite3_pcache_page,
    old_key: u32,
    new_key: u32,
) {
    let cache = unsafe { &mut *(cache as *mut Pager) };
    let _span = cache.span.enter();
    let _new_page = unsafe { &*(data as *mut Page) };
    tracing::trace!(
        old = old_key,
        new = new_key,
        old_page_key = _new_page.key(),
        page = cache.alloc.offset_of(_new_page),
        "rekey"
    );
    let mut pages = cache.pages.lock();
    let page = pages.remove(&old_key).expect("rekeyed key doesn't exist");
    page.pin(new_key);
    if let Some(page) = pages.insert(new_key, page) {
        assert!(!page.is_pinned());
        cache.alloc.free(page);
    }
}

extern "C" fn truncate(cache: *mut sqlite3_pcache, limit: u32) {
    let cache = unsafe { &*(cache as *mut Pager) };
    let _span = cache.span.enter();
    tracing::trace!(limit = limit, "truncate");
    let mut pages = cache.pages.lock();
    pages
        .extract_if(|k, _| *k >= limit)
        .for_each(|(_, p)| cache.alloc.free(p));
}

extern "C" fn destroy(cache: *mut sqlite3_pcache) {
    unsafe {
        let _ = Box::from_raw(cache as *mut Pager);
    }
}

extern "C" fn shrink(cache: *mut sqlite3_pcache) {
    let cache = unsafe { &mut *(cache as *mut Pager) };
    let _span = cache.span.enter();
    tracing::trace!("shrink");
    let mut pages = cache.pages.lock();
    pages
        .extract_if(|_, p| !p.is_pinned())
        .for_each(|(_, p)| cache.alloc.free(p));
}

pub const fn make_pager() -> sqlite3_pcache_methods2 {
    sqlite3_pcache_methods2 {
        iVersion: 2,
        pArg: std::ptr::null_mut(),
        xInit: Some(init),
        xShutdown: None,
        xCreate: Some(create),
        xCachesize: Some(cache_size),
        xPagecount: Some(page_count),
        xFetch: Some(fetch),
        xUnpin: Some(unpin),
        xRekey: Some(rekey),
        xTruncate: Some(truncate),
        xDestroy: Some(destroy),
        xShrink: Some(shrink),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn alloc_pages() {
        let alloc = Allocator::new(4096, 224, 3);
        let page1 = alloc.alloc().unwrap();
        assert!(!page1.is_free());
        assert_eq!(alloc.offset_of(page1), 0);
        assert_eq!(page1.p_page as usize, page1.data.as_ptr() as usize);
        assert_eq!(
            page1.p_extra as usize,
            page1.p_page as usize + LIBSQL_PAGE_SIZE as usize
        );
        let page2 = alloc.alloc().unwrap();
        assert_eq!(
            page2 as *mut _ as usize,
            page1 as *mut _ as usize + size_of::<Page>()
        );
        assert!(!page2.is_free());
        assert_eq!(alloc.offset_of(page2), 1);
        let page3 = alloc.alloc().unwrap();
        assert!(!page3.is_free());
        assert_eq!(alloc.offset_of(page3), 2);

        assert!(alloc.alloc().is_none());

        alloc.free(page2);

        let page4 = alloc.alloc().unwrap();
        assert_eq!(alloc.offset_of(page4), 1);
        assert!(alloc.alloc().is_none());
    }
}
