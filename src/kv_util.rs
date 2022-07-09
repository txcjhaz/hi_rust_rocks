use std::{hash::Hash, collections::{hash_map::DefaultHasher, HashMap}};

use rocksdb::WriteBatch;

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

pub struct RocksdbUtil<'a> {
    pub dbs: Vec<&'a rocksdb::DB>,
    pub size: u32
}

impl RocksdbUtil<'_> {

    fn getDb(&self, key: &str) -> &rocksdb::DB {
        let index = self.BKDRHash(key) % self.size;
        let db_opt = self.dbs.get(index as usize);
        
        match db_opt {
            Some(db) => {
                return db;
            }
            None => {
                return self.dbs[0];
            }
        }

    }

    fn BKDRHash(&self, str: &str) -> u32 {
        let seed: u32 = 131;
        let mut hash: u32 = 0;

        for ch in str.as_bytes() {
            hash = hash * seed + (*ch as u32);
        }

        hash & 0x7FFFFFFF
    }
 
    pub fn set(&self, _key: &str, _value: &str) {
        self.getDb(_key).put(_key, _value).unwrap();
    }

    pub fn get(&self, _key: &str) -> String {
       let s = self.getDb(_key).get(_key).unwrap().unwrap();
       let q = String::from_utf8(s).unwrap();
       return q;
    }

    pub fn remove(&self, _key: &str) {
        self.getDb(_key).delete(_key).unwrap();
    }

    pub fn mget(&self, _keys: &Vec<&str>) -> std::result::Result<&Vec<(usize, &Vec<u8>)>, &'static str> {
        let mut key_map: HashMap<u32, Vec<&str>> = HashMap::with_capacity(self.size as usize);
        for k in (*_keys) {
            let slot = self.BKDRHash(k);
            let opt = key_map.get_key_value(&slot);
            match opt {
                Some(v) => {
                    let mut vec = v.1;
                    vec.push(k);
                },
                None => {
                    key_map.insert(slot, vec![k]);
                }
            }
        }

        let mut mock_res = Vec::new();
        for _key in key_map {
            let (i, ks) = _key;
            let db = self.dbs[i as usize];
            let res = db.multi_get(ks);

            for (i, item) in res.iter().enumerate() {
                match item {
                    Ok(val) => {
                        match val {
                            Some(v) => {
                                mock_res.push((i, v));
                            },
                            None => {
                                return Err("404");
                            }
                        }
                    },
                    Err(_err) => {
                        return Err("404");
                    }
                }
            }
        }

        return Ok(&(mock_res.clone()));
    }

    pub fn mset(&self, kvs: &Vec<(&str, &str)>) {
        let key_map: HashMap<u32, Vec<(&str, &str)>> = 
            HashMap::with_capacity(self.size as usize);

        for kv in kvs {
            let slot = self.BKDRHash(kv.0);
            let opt = key_map.get_key_value(&slot);
            match opt {
                Some(v) => {
                    let mut vec = v.1;
                    vec.push(*kv);
                },
                None => {
                    key_map.insert(slot, vec![*kv]);
                }
            }
        }

        for _key in key_map {
            let db = self.dbs[_key.0 as usize];
            let mut batch = WriteBatch::default();
            for t in _key.1 {
                batch.put(t.0, t.1);
            }
            db.write(batch);
        }
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