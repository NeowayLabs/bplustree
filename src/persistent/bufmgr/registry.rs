use std::sync::Arc;

trait ManagedDataStructure {
    type PageValue;
    fn get_page_value(&self) -> Self::PageValue;
    fn find_parent(&self, page_value: Self::PageValue);
}

trait ErasedDataStructure {
    fn find_parent(&self);
}

impl <T, U: ManagedDataStructure<PageValue = T>> ErasedDataStructure for U {
    fn find_parent(&self) {
        self.find_parent(self.get_page_value())
    }
}



type DataStructureId = u64;

struct Registry {
    dt_map: scc::HashMap<DataStructureId, Arc<dyn ErasedDataStructure + Send + Sync>>
}
