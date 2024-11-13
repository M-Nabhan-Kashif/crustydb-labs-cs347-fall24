use crate::heap_page::HeapPage;
use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::{fs, num};

pub const STORAGE_DIR: &str = "heapstore";

// The data types we need for tracking the mapping between containerId and HeapFile/PathBuf
pub(crate) type ContainerMap = Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>;
pub(crate) type ContainerPathMap = Arc<RwLock<HashMap<ContainerId, Arc<PathBuf>>>>;
const PERSIST_CONFIG_FILENAME: &str = "storage_manager";

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_dir: PathBuf,
    /// Indicates if this is a temp StorageManager (for testing)
    is_temp: bool,
    pub(crate) cid_path_map: ContainerPathMap,
    #[serde(skip)]
    pub(crate) cid_heapfile_map: ContainerMap,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        // panic!("TODO milestone hs");

        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = heapfile_map.get(&container_id)?;
        match heap_file.read_page_from_file(page_id) {
            Ok(page) => Some(page),
            Err(_) => None,
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: &Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        // panic!("TODO milestone hs");

        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = heapfile_map.get(&container_id).ok_or_else(|| {
            CrustyError::CrustyError(format!("Container not found: {}", container_id))
        })?;
        // Do we need the error checking?

        heap_file.write_page_to_file(page)
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = heapfile_map
            .get(&container_id)
            .expect(&format!("Container not found: {}", container_id));

        heap_file.num_pages()
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let heapfile_map = self.cid_heapfile_map.read().unwrap();

        // Retrieve the HeapFile associated with the container_id,
        if let Some(heap_file) = heapfile_map.get(&container_id) {
            let read_count = heap_file.read_count.load(Ordering::Relaxed);
            let write_count = heap_file.write_count.load(Ordering::Relaxed);
            (read_count as u16, write_count as u16)
        } else {
            // container_id invalid, return (0, 0)
            (0, 0)
        }
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_dir as the location to persist data
    /// (if the storage manager persists records on disk)
    /// For startup/shutdown: check the storage_dir for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_dir: &Path) -> Self {
        let sm_file = storage_dir;
        let sm_file = sm_file.join(PERSIST_CONFIG_FILENAME);
        if sm_file.exists() {
            debug!("Loading storage manager from config file {:?}", sm_file);
            let reader = fs::File::open(sm_file).expect("error opening persist config file");
            let sm: StorageManager =
                serde_json::from_reader(reader).expect("error reading from json");

            let mut hm: HashMap<ContainerId, Arc<HeapFile>> = HashMap::new();
            let mut hmfiles: HashMap<ContainerId, Arc<PathBuf>> = HashMap::new();

            let path_map: ContainerPathMap = sm.cid_path_map.clone();
            let old_files = path_map.read().unwrap();

            for (id, path) in old_files.iter() {
                let hf = HeapFile::new(path.to_path_buf(), *id)
                    .expect("Error creating/opening old HF {path}");
                hmfiles.insert(*id, Arc::new(path.to_path_buf()));
                hm.insert(*id, Arc::new(hf));
            }

            let cid_heapfile_map = Arc::new(RwLock::new(hm));
            let cid_path_map = Arc::new(RwLock::new(hmfiles));
            StorageManager {
                storage_dir: storage_dir.to_path_buf(),
                cid_heapfile_map,
                cid_path_map,
                is_temp: false,
            }
        } else {
            debug!("Making new storage_manager in directory {:?}", storage_dir);
            let cid_heapfile_map = Arc::new(RwLock::new(HashMap::new()));
            let cid_path_map = Arc::new(RwLock::new(HashMap::new()));

            StorageManager {
                storage_dir: storage_dir.to_path_buf(),
                cid_heapfile_map,
                cid_path_map,
                is_temp: false,
            }
        }
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_dir = gen_random_test_sm_dir();
        debug!("Making new temp storage_manager {:?}", storage_dir);

        let cid_heapfile_map = Arc::new(RwLock::new(HashMap::new()));
        let cid_path_map = Arc::new(RwLock::new(HashMap::new()));

        StorageManager {
            storage_dir,
            cid_heapfile_map,
            cid_path_map,
            is_temp: true,
        }
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        let heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heapfile = heapfile_map.get(&container_id).expect("HeapFile not found");
        let new_page_id: u16 = heapfile.num_pages();

        // Iterate through pages and attempt insertion
        for page_id in 0..new_page_id {
            if let Ok(mut page) = heapfile.read_page_from_file(page_id) {
                if let Some(slot_id) = page.add_value(&value) {
                    heapfile.write_page_to_file(&page).unwrap();
                    return ValueId {
                        container_id,
                        segment_id: None,
                        page_id: Some(page_id),
                        slot_id: Some(slot_id),
                    };
                }
            }
        }

        // Creates a new page if needed
        let mut new_page = Page::new(new_page_id);
        let slot_id = new_page.add_value(&value).unwrap();
        heapfile.write_page_to_file(&new_page).unwrap();

        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(new_page_id),
            slot_id: Some(slot_id),
        }
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let cid_heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = match cid_heapfile_map.get(&id.container_id) {
            Some(heap_file) => heap_file,
            None => return Ok(()),
        };

        let page_id = id
            .page_id
            .ok_or_else(|| CrustyError::CrustyError("Invalid page_id".to_string()))?;
        let mut page = heap_file.read_page_from_file(page_id)?;

        if let Some(slot_id) = id.slot_id {
            if page.delete_value(slot_id).is_some() {
                heap_file.write_page_to_file(&page)?;
            }
        }

        Ok(())
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        self.delete_value(id.clone(), _tid)?;
        let new_value_id = self.insert_value(id.container_id, value, _tid);

        Ok(new_value_id)
    }

    /// Create a new container (i.e., a HeapFile) to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        let mut cid_path_map = self.cid_path_map.write().unwrap();
        let mut cid_heapfile_map = self.cid_heapfile_map.write().unwrap();

        if cid_path_map.contains_key(&container_id) {
            return Err(CrustyError::CrustyError(
                "Container id already exists".to_string(),
            ));
        }

        let mut path = self.storage_dir.clone();
        path.push(format!("cont_{}.hf", container_id));
        fs::create_dir_all(path.parent().unwrap()).expect("Failed to create directory");

        let curr_heapfile = HeapFile::new(path.clone(), container_id)?;
        cid_path_map.insert(container_id, Arc::new(path));
        cid_heapfile_map.insert(container_id, Arc::new(curr_heapfile));
        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, common::ids::StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted, remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut cid_heapfile_map = self.cid_heapfile_map.write().unwrap();
        if let Some(heap_file) = cid_heapfile_map.remove(&container_id) {
            let file_path = format!("heapfiles/container_{}.data", container_id);
            let path = PathBuf::from(&file_path);

            // Attempt to remove the file
            if path.exists() {
                std::fs::remove_file(&path).map_err(|error| {
                    CrustyError::CrustyError(format!(
                        "Failed to remove file: {} - {:?}",
                        file_path, error
                    ))
                })?;
            }
        }

        Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let cid_heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = cid_heapfile_map
            .get(&container_id)
            .expect("Container ID not found in cid_heapfile_map");

        HeapFileIterator::new(tid, Arc::clone(heap_file))
    }

    fn get_iterator_from(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
        start: ValueId,
    ) -> Self::ValIterator {
        let cid_heapfile_map = self.cid_heapfile_map.read().unwrap();
        let heap_file = match cid_heapfile_map.get(&container_id) {
            Some(hf) => Arc::clone(hf),
            None => panic!("Container ID not found in cid_heapfile_map"),
        };

        HeapFileIterator::new_from(tid, heap_file, start)
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let container_id = id.container_id;
        let cid_heapfile_map = self.cid_heapfile_map.read().unwrap();

        let heap_file = match cid_heapfile_map.get(&container_id) {
            Some(hf) => Arc::clone(hf),
            None => {
                return Err(CrustyError::CrustyError(
                    "Container ID not found".to_string(),
                ))
            }
        };

        // Retrieve the page, get the value from the specified slot
        let page_id = id
            .page_id
            .ok_or_else(|| CrustyError::CrustyError("Missing page_id".to_string()))?;
        let page = heap_file
            .read_page_from_file(page_id)
            .map_err(|_| CrustyError::CrustyError("Failed to read page".to_string()))?;

        if let Some(value) = page.get_value(
            id.slot_id
                .ok_or_else(|| CrustyError::CrustyError("Missing slot_id".to_string()))?,
        ) {
            Ok(value)
        } else {
            Err(CrustyError::CrustyError("Value not found".to_string()))
        }
    }

    fn get_storage_path(&self) -> &Path {
        &self.storage_dir
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_dir.clone())?;
        fs::create_dir_all(self.storage_dir.clone()).unwrap();

        self.cid_heapfile_map.write().unwrap().clear();
        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {}

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        debug!("serializing storage manager");
        let mut filename = self.storage_dir.clone();
        filename.push(PERSIST_CONFIG_FILENAME);
        serde_json::to_writer(
            fs::File::create(filename).expect("error creating file"),
            &self,
        )
        .expect("error serializing storage manager");
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_dir);
            let remove_all = fs::remove_dir_all(self.storage_dir.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
