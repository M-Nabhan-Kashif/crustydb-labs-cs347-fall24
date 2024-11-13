use crate::heap_page::HeapPage;
use crate::heap_page::HeapPageIntoIter;
use crate::heapfile::HeapFile;
use common::prelude::*;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    hf: Arc<HeapFile>,
    curr_page_id: PageId,
    curr_page_iter: Option<HeapPageIntoIter>,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let curr_page_iter = if 0 < hf.num_pages() {
            match hf.read_page_from_file(0) {
                Ok(page) => Some(page.into_iter()),
                Err(_) => None,
            }
        } else {
            None
        };

        HeapFileIterator {
            hf,
            curr_page_id: if curr_page_iter.is_some() { 1 } else { 0 },
            curr_page_iter,
        }
    }

    pub(crate) fn new_from(tid: TransactionId, hf: Arc<HeapFile>, value_id: ValueId) -> Self {
        let curr_page_iter = hf
            .read_page_from_file(value_id.page_id.unwrap())
            .ok()
            .map(|page| {
                let mut page_iter = page.into_iter();
                page_iter.nth(value_id.slot_id.unwrap() as usize); // Advances directly to start slot
                page_iter
            });

        HeapFileIterator {
            hf,
            curr_page_id: value_id.page_id.unwrap() + 1,
            curr_page_iter,
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((bytes, slot_id)) = self.curr_page_iter.as_mut()?.next() {
            return Some((
                bytes,
                ValueId {
                    container_id: self.hf.container_id,
                    segment_id: None,
                    page_id: Some(self.curr_page_id - 1),
                    slot_id: Some(slot_id),
                },
            ));
        }

        // Load the next page if current iterator is exhausted
        if self.curr_page_id < self.hf.num_pages() {
            self.curr_page_iter = match self.hf.read_page_from_file(self.curr_page_id) {
                Ok(page) => {
                    self.curr_page_id += 1;
                    Some(page.into_iter())
                }
                Err(_) => None,
            };

            // Attempt to fetch the next item from new page
            if let Some((bytes, slot_id)) = self.curr_page_iter.as_mut()?.next() {
                return Some((
                    bytes,
                    ValueId {
                        container_id: self.hf.container_id,
                        segment_id: None,
                        page_id: Some(self.curr_page_id - 1),
                        slot_id: Some(slot_id),
                    },
                ));
            }
        }

        None
    }
}
