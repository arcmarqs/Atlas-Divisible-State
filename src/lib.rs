use std::{mem, thread};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Error;
use atlas_common::crypto::hash::Context;
use atlas_common::ordering::{self, SeqNo};
use atlas_common::{crypto::hash::Digest, ordering::Orderable};

use atlas_metrics::metrics::{metric_duration, metric_store_count, metric_increment};
use atlas_smr_application::state::divisible_state::{StatePart, DivisibleStateDescriptor, PartId, DivisibleState};
use metrics::{CHECKPOINT_SIZE_ID, TOTAL_STATE_SIZE_ID};
use serde::{Deserialize, Serialize};
use sled::IVec;
use state_orchestrator::{StateOrchestrator, PREFIX_LEN, Prefix};
use state_tree::LeafNode;
use crate::metrics::CREATE_CHECKPOINT_TIME_ID;
use crate::state_orchestrator::get_range;

pub mod state_orchestrator;
pub mod state_tree;

pub mod metrics;

const CHECKPOINT_THREADS: usize = 2;

fn split_evenly<T>(slice: &[T], n: usize) -> impl Iterator<Item = &[T]> {
    struct Iter<'a, I> {
        pub slice: &'a [I],
        pub n: usize,
    }
    impl<'a, I> Iterator for Iter<'a, I> {
        type Item = &'a [I];
        fn next(&mut self) -> Option<&'a [I]> {
            if self.slice.len() == 0 {
                return None;
            }

            if self.n == 0 {
                return Some(self.slice);
            }
        
            let (first, rest) = self.slice.split_at(self.slice.len() / self.n);
            self.slice = rest;
            self.n -= 1;
            Some(first)
        }
    }

    Iter { slice, n }
}

#[derive(Debug,Clone, Serialize, Deserialize)]
pub struct SerializedState {
    leaf: Arc<LeafNode>,
    size: u64,
    bytes: Box<[u8]>,
}

impl SerializedState {
    pub fn from_prefix(prefix: Prefix, kvs: &[(Box<[u8]>,Box<[u8]>)]) -> Self {
        let size = (kvs.len() * mem::size_of_val(&kvs[0])) as u64;
        let bytes: Box<[u8]> = bincode::serialize(&kvs).expect("failed to serialize").into();

        //println!("bytes {:?}", bytes.len());
        //hasher.update(&pid.to_be_bytes());
        let mut hasher = Context::new();
        hasher.update(&bytes);

        Self {
            bytes,
            size,
            leaf: LeafNode::new(
                prefix,
                hasher.finish(),
            ).into(),
        }
    }

    pub fn to_pairs(&self) -> Box<[(Box<[u8]>,Box<[u8]>)]> {
        let kv_pairs: Box<[(Box<[u8]>,Box<[u8]>)]> = bincode::deserialize(&self.bytes).expect("failed to deserialize");

        kv_pairs
    }

    pub fn hash(&self) -> Digest {
        let mut hasher = Context::new();

        //hasher.update(&self.leaf.pid.to_be_bytes());
        hasher.update(&self.bytes);

        hasher.finish()
    }
}

impl StatePart<StateOrchestrator> for SerializedState {
    fn descriptor(&self) -> &LeafNode {
        self.leaf.as_ref()
    }

    fn id(&self) -> &[u8] {
        self.leaf.get_id()
    }

    fn length(&self) -> usize {
        self.bytes.len()
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    fn hash(&self) -> Digest {
        self.hash()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedTree {
    digest: Option<Digest>,
    seqno: SeqNo,
    leaves: Vec<Arc<LeafNode>>,
}

impl SerializedTree {
    pub fn new(digest: Option<Digest>, seqno: SeqNo, leaves: Vec<Arc<LeafNode>>) -> Self {
        Self {
            digest,
             seqno,
            leaves,
        }
    }
}

impl PartialEq for SerializedTree {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

 
impl Orderable for SerializedTree {
    fn sequence_number(&self) -> ordering::SeqNo {
        self.seqno
    }
}


impl DivisibleStateDescriptor<StateOrchestrator> for SerializedTree {
    fn parts(&self) -> Vec<Arc<LeafNode>>{
        self.leaves.iter().cloned().collect()
    }

    fn get_digest(&self) -> Option<Digest> {
        self.digest
    }
}

impl PartId for LeafNode {
    fn content_description(&self) -> &[u8] {
        self.get_digest()
    }

    fn id(&self) -> &[u8] {
        self.get_id()
    }
}

impl DivisibleState for StateOrchestrator {
    type PartDescription = LeafNode;
    type StateDescriptor = SerializedTree;
    type StatePart = SerializedState;

    fn get_descriptor(&self) -> SerializedTree {
        self.get_descriptor_inner()
    }

    fn accept_parts(&mut self, parts: Box<[Self::StatePart]>) -> atlas_common::error::Result<()> {
        let mut batch = sled::Batch::default();
        let mut tree_lock = self.mk_tree.write().expect("failed to write");
        let mut db_lock = self.db.0.write().expect("failed to write");

        for part in parts.iter() {
            let pairs = part.to_pairs();
            let prefix = part.id();
            for (k,v) in pairs.iter() {
                let (k,v) = ([prefix,k.as_ref()].concat(), v.to_vec());
                 db_lock.insert(k, v) ;
            }   

            tree_lock.insert_leaf( Prefix::new(prefix), part.leaf.clone());

            
        }
        
        drop(db_lock);
        drop(tree_lock);
       // self.db.0.apply_batch(batch).expect("failed to apply batch");
        
        //let _ = self.db.flush();

        Ok(())
    }

    fn get_parts(
        &mut self
    ) -> Result<Vec<SerializedState>, Error> {
       metric_store_count(CHECKPOINT_SIZE_ID, 0);
       metric_store_count(TOTAL_STATE_SIZE_ID, 0);

        let process_part = |(k,v) : (&Vec<u8>,&Vec<u8>)| {

            metric_increment(CHECKPOINT_SIZE_ID, Some((k.len() + v.len()) as u64));

            (k[PREFIX_LEN..].into(), v.deref().into())
        };

        let checkpoint_start = Instant::now();

        if self.updates.is_empty() {
            metric_duration(CREATE_CHECKPOINT_TIME_ID, checkpoint_start.elapsed());
           // metric_increment(TOTAL_STATE_SIZE_ID, Some(self.db.0.size_on_disk().expect("failed to get size")));

            return Ok(vec![])
        }

        let state_parts = Arc::new(Mutex::new(Vec::new()));

        let parts = self.updates.extract();
        println!("updates {:?}", parts.len());
 
        let chunks = split_evenly(parts.clone().as_slice(), CHECKPOINT_THREADS).map(|chunk| chunk.to_owned()).collect::<Vec<_>>();
        let mut handles = vec![];
        for chunk in chunks {
            let db_handle = self.db.0.clone();
            let state_parts = state_parts.clone();
            let tree = self.mk_tree.clone();
            let handle = thread::spawn(move || {
                let mut local_state_parts = Vec::new();
                    let db_lock = db_handle.read().expect("failed to read");
                    for prefix in chunk {
                        let r = get_range(&prefix);
                        println!("iter size {:?} {:?}",r.0, r.1);
                        let kv_iter = db_lock.range(r.0 .. r.1).collect::<Vec<_>>();
                        let kv_pairs  = kv_iter.into_iter()
                        .map(process_part).collect::<Box<_>>();
                        if kv_pairs.is_empty() {
                            continue;
                        }
                        let serialized_part = SerializedState::from_prefix(prefix.clone(),kv_pairs.as_ref());
                        local_state_parts.push(serialized_part);
                    } 

                    tree.write().expect("failed to write").leaves.extend(local_state_parts.iter().map(|part| (Prefix::new(part.id()), part.leaf.clone())));
                    state_parts.lock().expect("failed to lock").extend(local_state_parts.into_iter());  
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
     /*    for prefix in parts {
            println!("{:?}", prefix);
            let kv_iter = self.db.0.scan_prefix(prefix.as_ref());
            let kv_pairs  = kv_iter
            .map(|kv| kv.map(process_part).expect("failed to process part") )
            .collect::<Box<_>>();
            if kv_pairs.is_empty() {
                continue;
            }
            let serialized_part = SerializedState::from_prefix(prefix.clone(),kv_pairs.as_ref());
            state_parts.push(serialized_part);
        }*/

        let parts_lock = Arc::try_unwrap(state_parts).expect("Lock still has multiple owners");
        let parts = parts_lock.into_inner().expect("Lock still has multiple owners");
        //println!("descriptor {:?}", self.mk_tree.read().expect("failed to read").leaves.values().map(|v| v.digest).collect::<Vec<_>>());

        self.mk_tree.write().expect("failed to write").calculate_tree();
        //println!("parts {:?}", state_parts);


        metric_duration(CREATE_CHECKPOINT_TIME_ID, checkpoint_start.elapsed());
       // metric_increment(TOTAL_STATE_SIZE_ID, Some(self.db.0.size_on_disk().expect("failed to get size")));
        
       //println!("state size {:?}", self.db.0.expect("failed to read size"));
      //  println!("checkpoint size {:?}",  state_parts.iter().map(|f| mem::size_of_val(*&(&f).bytes()) as u64).sum::<u64>());
        Ok(parts)
    }

 /*    fn get_seqno(&self) -> atlas_common::error::Result<SeqNo> {
        Ok(self.mk_tree.read().expect("failed to read").get_seqno())
    } */

    fn finalize_transfer(&mut self) -> atlas_common::error::Result<()> {           
        metric_store_count(TOTAL_STATE_SIZE_ID, 0);
        self.mk_tree.write().expect("failed to get write").calculate_tree();
        println!("post state transfer tree {:?}", self.get_descriptor().digest);
        
        //metric_increment(TOTAL_STATE_SIZE_ID, Some(self.db.0.size_on_disk().expect("failed to get size")));

        //println!("finished st {:?}", self.get_descriptor());

        //println!("Verifying integrity");

       //self.db.0.verify_integrity().expect("integrity check failed");

        Ok(())
    } 
}
