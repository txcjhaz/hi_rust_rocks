use std::{hash::{Hash, Hasher}, collections::{hash_map::DefaultHasher, HashMap}, sync::{Arc, Mutex}};

const TOTAL_SLOTS: usize = 1024;
const SLOT_SIZE: usize = 20000;
pub trait KvUtil {
    fn set(&self, key: &str, value: &str);
    fn get(&self, key: &str) -> &str;
    fn remove(&self, key: &str);
    fn mget(&self, keys:  &Vec<&str>) -> Vec<&str>;
    fn mset(&self, keys: &Vec<&str>, vals: &Vec<&str>);
    fn zadd(&self, key: &str, vals: &str, scores: &u32);
    fn zrange(&self, key: &str, min_score: &u32, max_score: &u32) -> &str;
    fn zrmv(&self, key: &str, value: &str);
}

pub struct MockKvUtil {}
impl KvUtil for MockKvUtil {
    fn set(&self, _key: &str, _value: &str) {
        "set success";
    }

    fn get(&self, _key: &str) -> &str {
        "get success"
    }

    fn remove(&self, _key: &str) {
        "get success";
    }

    fn mget(&self, _keys: &Vec<&str>) -> Vec<&str> {
        let mut mock_res = Vec::new();
        mock_res.push("value");
        mock_res
    }

    fn mset(&self, _key: &Vec<&str>, _vals: &Vec<&str>) {
        "get success";
    }

    fn zadd(&self, _key: &str, _val: &str, _score: &u32) {
        "get success";
    }

    fn zrange(&self, _key: &str, _min_score: &u32, _max_score: &u32) -> &str {
        "get success"
    }

    fn zrmv(&self, _key: &str, _val: &str) {
        "get success";
    }
}

type Db = Arc<Mutex<HashMap<String, String>>>;

pub struct SelfKvUtil {
    pub dbs: Vec<Db>
}

impl SelfKvUtil {

    fn get_db(&self, key: &str) -> &Db {
        let index = self.simple_hash(key) % TOTAL_SLOTS as u32;
        let db_opt = self.dbs.get(index as usize);
        
        match db_opt {
            Some(db) => {
                return db;
            }
            None => {
                return &self.dbs[0];
            }
        }

    }

    fn simple_hash(&self, str: &str) -> u32 {
        let mut hasher = DefaultHasher::new();
        hasher.write(str.as_bytes());
        hasher.finish() as u32
    }

    fn bdkr_hash(&self, str: &str) -> u32 {
        let seed: u32 = 131;
        let mut hash: u32 = 0;

        for ch in str.as_bytes() {
            hash = hash * seed + (*ch as u32);
        }

        hash & 0x7FFFFFFF
    }
 
    fn check_capacity(size: usize) -> Result<(), String> {
        if size < SLOT_SIZE {
            Ok(())
        } else {
            Err(String::from("out of capacity"))
        }
    }

    pub fn set(&self, _key: &str, _value: &str) -> Result<(), String> {
        let mut db = self.get_db(_key).lock().unwrap();

        if let Ok(_) = SelfKvUtil::check_capacity(db.len() + 1) {
            db.insert(_key.to_string(), _value.to_string());
            return Ok(());
        } else {
            return Err("out of capacity".to_string());
        }
    }

    pub fn get(&self, _key: &str) -> Option<String> {
        let db = self.get_db(_key).lock().unwrap();
        if let Some(val) = db.get(_key) {
            Some((*val).clone())
        } else {
            None
        }
    }

    pub fn remove(&self, _key: &str) {
        let mut db = self.get_db(_key).lock().unwrap();
        db.remove(_key);
    }

    pub fn mget(&self, _keys: &Vec<&str>) -> Result<Vec<(String, String)>, String> {
        let mut res = Vec::new();

        for _key in (*_keys).clone() {
            let db = self.get_db(_key).lock().unwrap();
            if let Some(val) = db.get(_key) {
                res.push((_key.to_string(), (*val).clone()));
            } else {
                return Err("not found".to_string());
            }
        }

        return Ok(res);
    }

    pub fn mset(&self, kvs: &Vec<(&str, &str)>) -> Result<(), String> {
        for kv in (*kvs).clone() {
            let mut db = self.get_db(kv.0).lock().unwrap();
            
            if let Ok(_) = SelfKvUtil::check_capacity(db.len() + 1) {
                db.insert(kv.0.to_string(), kv.1.to_string());
            } else {
                return Err("out of capacity".to_string());
            }
        }

        Ok(())
    }

    pub fn zadd(&self, _key: &str, _val: &str, _score: &u32) {
        "get success";
    }

    pub fn zrange(&self, _key: &str, _min_score: &u32, _max_score: &u32) -> &str {
        "get success"
    }

    pub fn zrmv(&self, _key: &str, _val: &str) {
        "get success";
    }
}