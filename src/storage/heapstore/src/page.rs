pub use crate::heap_page::HeapPage;
use common::prelude::*;
use common::PAGE_SIZE;
use std::fmt;
use std::fmt::Write;

// Type to hold any value smaller than the size of a page.
// We choose u16 because it is sufficient to represent any slot that fits in a 4096-byte-sized page.
// Note that you will need to cast Offset to usize if you want to use it to index an array.
pub type Offset = u16;
// For debug
const BYTES_PER_LINE: usize = 40;
const SLOT_SIZE: usize = 6;

/// Page struct. This must occupy not more than PAGE_SIZE when serialized.
/// In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// If you delete a value, you do not need reclaim header space the way you must reclaim page
/// body space. E.g., if you insert 3 values then delete 2 of them, your header can remain 26
/// bytes & subsequent inserts can simply add 6 more bytes to the header as normal.
/// The rest must filled as much as possible to hold values.
pub struct Page {
    /// The data for data
    pub(crate) data: [u8; PAGE_SIZE],
}

/// The functions required for page
impl Page {
    /// Create a new page
    /// HINT: To convert a variable x to bytes using little endian, use
    /// x.to_le_bytes()
    pub fn new(page_id: PageId) -> Self {
        let mut page = Page {
            data: [0; PAGE_SIZE],
        };

        // Page ID in first 2 bytes
        let page_id_bytes = page_id.to_le_bytes();
        page.data[0..2].copy_from_slice(&page_id_bytes);

        // Starting offset and slot count initialized
        page.set_offset(PAGE_SIZE);
        page.data[4..6].clone_from_slice(&(0 as u16).to_le_bytes());

        page
    }

    /// Return the page id for a page
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    pub fn get_page_id(&self) -> PageId {
        PageId::from_le_bytes(self.data[0..2].try_into().unwrap())
    }

    /// Create a page from a byte array
    #[allow(dead_code)]
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Self {
        Page { data }
    }

    /// Get a reference to the bytes of the page
    pub fn to_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Utility function for comparing the bytes of another page.
    /// Returns a vec  of Offset and byte diff
    #[allow(dead_code)]
    pub fn compare_page(&self, other_page: Vec<u8>) -> Vec<(Offset, Vec<u8>)> {
        let mut res = Vec::new();
        let bytes = self.to_bytes();
        assert_eq!(bytes.len(), other_page.len());
        let mut in_diff = false;
        let mut diff_start = 0;
        let mut diff_vec: Vec<u8> = Vec::new();
        for (i, (b1, b2)) in bytes.iter().zip(&other_page).enumerate() {
            if b1 != b2 {
                if !in_diff {
                    diff_start = i;
                    in_diff = true;
                }
                diff_vec.push(*b1);
            } else if in_diff {
                //end the diff
                res.push((diff_start as Offset, diff_vec.clone()));
                diff_vec.clear();
                in_diff = false;
            }
        }
        res
    }

    /// PAGE METADATA

    /// Setting the offest value/storing in bytes 3 and 4
    pub fn set_offset(&mut self, offset:usize) {
        self.data[2..4].clone_from_slice(&(offset as u16).to_le_bytes());
    }

    /// Getting the free space offset from bytes 3 and 4
    pub fn get_offset(&self) -> usize {
        u16::from_le_bytes(self.data[2..4].try_into().unwrap()) as usize
    }

    /// Getting the slot count from bytes 5 and 6
    pub fn get_slot_count(&self) -> usize {
        u16::from_le_bytes(self.data[4..6].try_into().unwrap()) as usize
    }

    /// Increments slot count at bytes 5-6 by 1, easy use for insertion.
    pub fn increment_slot_count(&mut self) {
        let current_count = self.get_slot_count();
        self.data[4..6].clone_from_slice(&((current_count + 1) as u16).to_le_bytes());
    }

    /// SLOT METADATA

    /// First 2 bytes of slot metadata for each slot id has length
    pub fn set_slot_length(&mut self, id: SlotId, length: usize) {
        let start = 8 + id as usize * SLOT_SIZE;
        let end = start + 2;
        self.data[start..end].copy_from_slice(&(length as u16).to_le_bytes())
    }

    /// Retrieve slot length
    pub fn get_slot_length(&self, id: SlotId) -> usize {
        let start = 8 + id as usize * SLOT_SIZE;
        let end = start + 2;
        u16::from_le_bytes(self.data[start..end].try_into().unwrap()) as usize
    }

    /// Bytes 3-4 of slot metadata hold slot offset
    pub fn set_slot_offset(&mut self, id: SlotId, offset: usize) {
        let start = 10 + id as usize * SLOT_SIZE;
        let end = start + 2;
        self.data[start..end].copy_from_slice(&(offset as u16).to_le_bytes())
    }

    /// Retrieve slot offset
    pub fn get_slot_offset(&self, id: SlotId) -> usize {
        let start = 10 + id as usize * SLOT_SIZE;
        let end = start + 2;
        u16::from_le_bytes(self.data[start..end].try_into().unwrap()) as usize
    }

    /// Sets slot used value to 1 for used
    pub fn set_slot_used(&mut self, id: SlotId) {
        let start = 12 + id as usize * SLOT_SIZE;
        let end = start + 2;
        self.data[start..end].clone_from_slice(&1u16.to_le_bytes());
    }

    /// Sets slot used value to 0 for unused
    pub fn set_slot_unused(&mut self, id: SlotId) {
        let start = 12 + id as usize * SLOT_SIZE;
        let end = start + 2;
        self.data[start..end].clone_from_slice(&0u16.to_le_bytes()); // 0 means "unused"
    }

    /// Function returns true if slot is used or false if unused
    pub fn get_slot_status(&self, id: SlotId) -> bool {
        let start = 12 + id as usize * SLOT_SIZE;
        let end = start + 2;
        let status = u16::from_le_bytes(self.data[start..end].try_into().unwrap());
        status == 1
    }

    /// Writing Data to slot

    /// Writing data to either an existing slot or the next slot in a linear sequencing
    pub fn fill_slot_at(&mut self, slot_id: SlotId, bytes: &[u8]) -> Option<SlotId> {
        
        let value_size = bytes.len();
        let offset = self.get_offset();

        if offset < self.get_header_size() {
            // Not enough space to store the value
            return None;
        }
        self.data[offset - value_size..offset].clone_from_slice(bytes);
        // Metadata update
        self.set_slot_length(slot_id, value_size);
        self.set_slot_offset(slot_id, offset);
        self.set_slot_used(slot_id);
        // Page offset updated
        self.set_offset(offset - value_size);
        return Some(slot_id);
    }

}

impl Clone for Page {
    fn clone(&self) -> Self {
        Page { data: self.data }
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //let bytes: &[u8] = unsafe { any_as_u8_slice(&self) };
        let p = self.to_bytes();
        let mut buffer = String::new();
        let len_bytes = p.len();

        writeln!(&mut buffer, "PID:{}", self.get_page_id()).unwrap();
        let mut pos = 0;
        let mut remaining;
        let mut empty_lines_count = 0;
        let comp = [0; BYTES_PER_LINE];
        //hide the empty lines
        while pos < len_bytes {
            remaining = len_bytes - pos;
            if remaining > BYTES_PER_LINE {
                let pv = &(p)[pos..pos + BYTES_PER_LINE];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    write!(&mut buffer, "{} ", empty_lines_count).unwrap();
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                // for hex offset
                write!(&mut buffer, "[{:4}] ", pos).unwrap();
                #[allow(clippy::needless_range_loop)]
                for i in 0..BYTES_PER_LINE {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => write!(&mut buffer, "{:02x} ", pv[i]).unwrap(),
                    };
                }
            } else {
                let pv = &(*p)[pos..pos + remaining];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    write!(&mut buffer, "{} ", empty_lines_count).unwrap();
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                // for hex offset
                //buffer += &format!("[0x{:08x}] ", pos);
                write!(&mut buffer, "[{:4}] ", pos).unwrap();
                #[allow(clippy::needless_range_loop)]
                for i in 0..remaining {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => write!(&mut buffer, "{:02x} ", pv[i]).unwrap(),
                    };
                }
            }
            buffer += "\n";
            pos += BYTES_PER_LINE;
        }
        if empty_lines_count != 0 {
            write!(&mut buffer, "{} ", empty_lines_count).unwrap();
            buffer += "empty lines were hidden\n";
        }
        write!(f, "{}", buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;
    use rand::Rng;

    /// Limits how on how many bytes we can use for page metadata / header

    #[test]
    fn hs_page_create_basic() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());

        let p = Page::new(1);
        assert_eq!(1, p.get_page_id());

        let p = Page::new(1023);
        assert_eq!(1023, p.get_page_id());
    }
}
