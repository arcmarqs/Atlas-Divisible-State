use std::{collections::{ vec_deque, BTreeMap, BTreeSet}, ops::Range, sync::{Arc, RwLock}};

use crate::{
    state_tree::StateTree,
    SerializedTree,
};
use atlas_common::{collections::HashSet, ordering::SeqNo};
use concurrent_map::ConcurrentMap;
use serde::{Deserialize, Serialize};
use log::{debug, error, info, trace, warn};
include!("generated.rs");

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize,Hash)]
pub struct Prefix(pub [u8;PREFIX_LEN]);

impl Prefix {
    pub fn new(prefix: &[u8]) -> Prefix {
        Self(prefix.try_into().expect("failed to create array"))
    }

    pub fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
   // pub fn truncate(&self, len: usize) -> Prefix {
  //      let new_prefix = self.0[..len];
//
  //      Prefix(new_prefix)
 //   }
}
// A bitmap that registers changed prefixes over a set of keys
#[derive(Debug, Clone)]
pub struct PrefixSet {
    pub prefixes: BTreeSet<Prefix>,
    pub seqno: SeqNo,
}

impl Default for PrefixSet {
    fn default() -> Self {
        Self { prefixes: Default::default(), seqno: SeqNo::ZERO }
    }
}

impl PrefixSet {
    pub fn new() -> PrefixSet {
        Self { 
            prefixes: BTreeSet::default(), 
            seqno: SeqNo::ZERO,
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        // if a prefix corresponds to a full key we can simply use the full key
        
        let prefix = Prefix::new(&key[..PREFIX_LEN]);

      //  if self.prefixes.is_empty() {
      //      self.prefix_len = prefix.0.len();
      //      self.prefixes.insert(prefix);
      //  } else {
            self.prefixes.insert(prefix);
            self.seqno = self.seqno.next();
       // }

       // if self.prefixes.len() >= 8000 {
       //     println!("merging");
       //     self.merge_prefixes();
       // }
    }

    pub fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.prefixes.len()
    }

    pub fn clear(&mut self) {
        self.seqno = SeqNo::ZERO;
        self.prefixes.clear();
       // self.prefix_len = 0;
    }

    pub fn extract(&mut self) -> Vec<Prefix> {
        let vec = self.prefixes.iter().cloned().collect::<Vec<_>>();
        self.clear();
        vec
    }
   // fn merge_prefixes(&mut self) {
  //      self.prefix_len -= 1;
  //      let mut new_set: BTreeSet<Prefix> = BTreeSet::new();
  //      for prefix in self.prefixes.iter() {
  //          new_set.insert(prefix.truncate(self.prefix_len));
   //     }
   //     self.prefixes = new_set;
   // }
}

#[derive(Debug,Clone)]
pub struct DbWrapper(pub Arc<RwLock<BTreeMap<Vec<u8>,Vec<u8>>>>);

impl Default for DbWrapper {
    fn default() -> Self {
        Self (Arc::new(RwLock::new(BTreeMap::default())))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateOrchestrator {
    #[serde(skip_serializing, skip_deserializing)]
    pub db: DbWrapper,
    #[serde(skip_serializing, skip_deserializing)]
    pub updates: PrefixSet,
    #[serde(skip_serializing, skip_deserializing)]
    pub mk_tree: Arc<RwLock<StateTree>>,
    #[serde(skip_serializing, skip_deserializing)]
    pub keylen: usize,
}

impl StateOrchestrator {
    pub fn new(path: &str, keylen: usize) -> Self {
     /*   let conf = Config::new()
        .mode(Mode::HighThroughput)
        .temporary(true)
        .path(path);*/ 
        
        let ret = Self {
            db: DbWrapper::default(),
            updates: PrefixSet::default(),
            mk_tree: Arc::new(RwLock::new(StateTree::default())),
            keylen,
        };

       ret
    }
  /*   pub fn get_subscriber(&self) -> Subscriber {
        self.db.0.watch_prefix(vec![])
    }
*/
    pub fn insert(&mut self, key: &[u8], value: Vec<u8>) -> Option<Vec<u8>> {
        let ret =  self.db.0.write().expect("failed to write").insert(key.to_vec(), value);
        self.updates.insert(&key);
        ret  
    }

    pub fn remove(&mut self, key: &[u8])-> Option<Vec<u8>> {
        let res = self.db.0.read().expect(" failed to read").get(key).cloned();
        if res.is_some(){
            self.updates.insert(key);
            let _ = self.db.0.write().expect("failed to write").insert(key.to_vec(), vec![]);
        }
        res
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.0.read().expect(" failed to read").get(key).cloned()
    }


    pub fn get_descriptor_inner(&self) -> SerializedTree {
        let lock = self.mk_tree.read().expect("failed to read");
    
        SerializedTree { digest: lock.root, leaves: lock.leaves.values().cloned().collect::<Vec<_>>(), seqno: lock.seqno  }
    }

}

pub fn get_range(prefix: &Prefix,keylen: usize) -> (Vec<u8>, Vec<u8>) {
    let mut st = vec![0;keylen-PREFIX_LEN];
    //println!("fill {:?}", st);
    let start = [prefix.0.as_slice(),st.as_slice()].concat();
    st = vec![255;keylen-PREFIX_LEN];
    let end = [prefix.0.as_slice(),st.as_slice()].concat();
    //println!("end {:?}", st);

    (start,end)
}
