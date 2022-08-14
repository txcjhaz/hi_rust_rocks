use std::{ fmt::Write, collections::{HashMap, hash_map::RandomState}, sync::{Arc, Mutex}, hash::{Hash, Hasher}, io::{self, Read}, fs::OpenOptions};

use bytes::{Bytes, BufMut, BytesMut};
use log::{info, debug};
use may_minihttp::{HttpService, HttpServiceFactory, Request, Response, SelfKvUtil};
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use lazy_static::lazy_static;
use core::hash::BuildHasher;
use lru::LruCache;

extern crate serde;

const TOTAL_SLOTS: usize = 1 << 12;
const SLOT_SIZE: usize = 20000;

struct Techempower {}

#[derive(Deserialize, Serialize, Debug)]
struct KeyValue<'a> {
    key: &'a str,
    value: &'a str
}

#[derive(Deserialize, Serialize, Debug)]
struct KeyValueRes<'a> {
    key: &'a str,
    value: String
}


#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct ScoreValue {
    score: u32,
    value: String,
}


#[derive(Deserialize, Serialize, Debug)]
struct ZRange {
    min_score: u32,
    max_score: u32
}


const MEM_USED: usize = 14 << 30;
const SHARD_SIZE: usize = 256;
const SHARD_SIZE_64: u64 = SHARD_SIZE as u64;
const KV_MAX_SIZE: usize = 200;
const SHARD_CAPACITY: usize = MEM_USED / SHARD_SIZE / KV_MAX_SIZE;


impl HttpService for Techempower {

    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        // Bare-bones router
        if req.path() == "/init" {
            rsp.header("Content-Type: text/plain").body("ok");
        }
        else if req.path().starts_with("/query/") {
            let key = &req.path()[7..];
            if let Some(val) = SHARDED_LRU.get(key) {
                rsp.body_mut().put(val.as_ref());
            } else {
                rsp.status_code("404", "");
            }
        }
        else if req.path() == "/add" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());
            let json_parse_resp: Result<KeyValue, serde_json::Error> = serde_json::from_slice(r_body); 
            match json_parse_resp {
                Ok(kv) => {
                    SHARDED_LRU.set(kv.key, kv.value);
                },
                Err(_err) => {
                    rsp.status_code("400", "");
                    return Ok(());
                }
            }         
            // println!("to add key is {}, value is {}", kv.key, kv.value);
        }
        else if req.path().starts_with("/del/") {
            let key = &req.path()[5..];
            SHARDED_LRU.remove(key);
            // println!("del key is {}", key);
        }
        else if req.path() == "/list" {
            let r_body = req.body_();
            let json_parse_resp: Result<Vec<&str>, serde_json::Error> = 
                serde_json::from_slice(r_body);
            
            let b = rsp.body_mut();
            match json_parse_resp {
                Ok(keys) => {
                    let mut final_res = Vec::<KeyValueRes>::with_capacity(keys.len());
                    for key in keys {
                        if let Some(val) = SHARDED_LRU.get(key) {
                            final_res.push(KeyValueRes{
                                key: key,
                                value: String::from_utf8(val.to_vec()).unwrap()
                            });
                        } else {
                            rsp.status_code("404", "");
                        }
                    }

                    let body_mut = rsp.body_mut();
                    let r = serde_json::to_string(&final_res).unwrap();
                    body_mut.write_str(&r).unwrap();
            
                },
                Err(_err) => {
                    rsp.status_code("400", "2");
                }
            }
        }
        else if req.path() == "/batch" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());

            let kv_parse_rsp: Result<Vec<KeyValue>, serde_json::Error> = serde_json::from_slice(r_body); 
            match kv_parse_rsp {
                Ok(kvs) => {
                    for kv in kvs {
                        SHARDED_LRU.set(kv.key, kv.value);
                    }
                },
                Err(_err) => {
                    rsp.status_code("400", "");
                    return Ok(());
                }
            }
        }
        else if req.path().starts_with("/zadd/") {
            let key = &req.path()[6..];
            let data: Result<ScoreValue, serde_json::Error> = serde_json::from_slice(req.body_());
            match data {
                Ok(score_value) => {
                    debug!("key is {}, value is {}", key, score_value.value);
                    SHARDED_LRU_ZSET.zadd(&key, score_value.clone());
                }
                Err(_) => {
                    rsp.status_code("400", "");
                    return Ok(());
                }
            }
        }
        else if req.path().starts_with("/zrange/") {
            let key = &req.path()[8..];
            let data: Result<ZRange, serde_json::Error> = serde_json::from_slice(req.body_());
                    match data {
                        Ok(zrange) => {
                            debug!("key is {}, min_score is {}", key, zrange.min_score);
                            let res = SHARDED_LRU_ZSET.zrange(&key, zrange);

                            if res.is_none() {
                                rsp.status_code("404", "");
                                return Ok(());
                            }

                            let str = serde_json::to_string(&res);
                            match str {
                                Ok(x) => {
                                    rsp.body_mut().write_str(&x);
                                },
                                Err(_) => {
                                    rsp.status_code("400", "");
                                    return Ok(());
                                }
                            }
                            return Ok(());
                        }
                        Err(_) => {}
                    }

        }
        else if req.path().starts_with("/zrmv/") {
            let caps = P_ZRM_REGEX.captures(&req.path().as_bytes()).unwrap().unwrap();
            let key = std::str::from_utf8(&caps["key"]).unwrap();
            let value = std::str::from_utf8(&caps["value"]).unwrap();
            SHARDED_LRU_ZSET.zrmv(key, value);
        }
        else {
            rsp.status_code("404", "Not Found");
        }

        Ok(())
    }
}

struct HttpServer {}

impl HttpServiceFactory for HttpServer {
    type Service = Techempower;

    fn new_service(&self) -> Self::Service {
        // let kv_util_impl = MockKvUtil {};
        Techempower {}
    }

}



fn main() {
    // init aof
    if let Ok(file) = std::fs::File::open("aof.csv") {}
    else {
        info!("aof file not exist, try to create.");
        std::fs::File::create("aof.csv");
    } 

    // init cache
    SHARDED_LRU.set_unsave("llll", "我是卖报的小画家");
    SHARDED_LRU_ZSET.zadd("llll", ScoreValue{ score: 2, value: "我是卖报的小画家".to_string() });

    may::config()
        .set_pool_capacity(10000)
        .set_stack_size(0x1000)
        .set_workers(4);

    // let buildhasher = SelfBuilder{};

    // let map: Arc<DashMap<String, String, SelfBuilder>> = Arc::new(DashMap::with_capacity_and_hasher_and_shard_amount(16<<10/100/4, buildhasher, 4));
    // map.insert("key".to_string(), "value".to_string());


    let http_server = HttpServer {};
    let server = http_server.start("0.0.0.0:8080").unwrap();

    server.join().unwrap();
}
// pub fn func<S: fnv::FnvBuildHasher>(map: DashMap<String, String, S>) {}



lazy_static! {
    static ref P_ZRM_REGEX: pcre2::bytes::Regex = pcre2::bytes::Regex::new(r#"/zrmv/(?P<key>.*)/(?P<value>.*)"#).unwrap();

    static ref SHARDED_LRU: ShardLRU = {
        let mut dbs = Vec::<Arc<Mutex<LruCache<String, Bytes, fnv::FnvBuildHasher>>>>::with_capacity(SHARD_SIZE);
        for i in 0..SHARD_SIZE {
            dbs.push(Arc::new(Mutex::new(
                LruCache::<String, Bytes, fnv::FnvBuildHasher>::with_hasher(SHARD_CAPACITY, fnv::FnvBuildHasher::default())
            )));
        }

        ShardLRU {
            dbs: dbs
        }
    };

    static ref SHARDED_LRU_ZSET: ShardLRUZSet = {
        let mut dbs = Vec::<Arc<Mutex<LruCache<String, sorted_vec::SortedSet<ScoreValue>, fnv::FnvBuildHasher>>>>::with_capacity(SHARD_SIZE);
        for i in 0..SHARD_SIZE {
            dbs.push(Arc::new(Mutex::new(
                LruCache::<String, sorted_vec::SortedSet<ScoreValue>, fnv::FnvBuildHasher>::with_hasher(SHARD_CAPACITY, fnv::FnvBuildHasher::default())
            )));
        }

        ShardLRUZSet {
            dbs: dbs
        }
    };

}

struct ShardLRU {
    dbs: Vec<Arc<Mutex<LruCache<String, Bytes, fnv::FnvBuildHasher>>>>,
}

struct ShardLRUZSet {
    dbs: Vec<Arc<Mutex<LruCache<String, sorted_vec::SortedSet<ScoreValue>, fnv::FnvBuildHasher>>>>,
}

impl ShardLRU {
    fn get_db(&self, key: &str) -> &Arc<Mutex<LruCache<String, Bytes, fnv::FnvBuildHasher>>> {
        let mut hasher = fnv::FnvHasher::default();
        hasher.write(key.as_ref());
        let slot = hasher.finish() % SHARD_SIZE_64;
        if let Some(db) = self.dbs.get(slot as usize) {
            db
        } else {
            &self.dbs[0]
        }
    }

    fn get(&self, key: &str) -> Option<Bytes> {
        let mut db = self.get_db(key).lock().unwrap();
        if let Some(val) = db.get(key) {
            Some((*val).clone())
        } else {
            None
        }
    }

    fn set(&self, key: &str, val: &str) {
        let mut db = self.get_db(key).lock().unwrap();
        db.put(key.to_owned(), Bytes::from(val.to_owned()));
        // write_to_aof_buffer(key, val);
    }

    fn set_unsave(&self, key: &str, val: &str) {
        let mut db = self.get_db(key).lock().unwrap();
        db.put(key.to_owned(), Bytes::from(val.to_owned()));
    }

    fn remove(&self, key: &str) {
        let mut db = self.get_db(key).lock().unwrap();
        db.pop(key);
    }
}

impl ShardLRUZSet {
    fn get_db(&self, key: &str) -> &Arc<Mutex<LruCache<String, sorted_vec::SortedSet<ScoreValue>, fnv::FnvBuildHasher>>> {
        let mut hasher = fnv::FnvHasher::default();
        hasher.write(key.as_ref());
        let slot = hasher.finish() % SHARD_SIZE_64;
        if let Some(db) = self.dbs.get(slot as usize) {
            db
        } else {
            &self.dbs[0]
        }
    }

    fn zadd(&self, key: &str, scoreValue: ScoreValue) {
        let mut db = self.get_db(key).lock().unwrap();
        if let Some(set) = db.get_mut(key) {
            set.find_or_insert(scoreValue);
        } else {
            let mut set = sorted_vec::SortedSet::new();
            set.find_or_insert(scoreValue);
            db.push(key.to_string(), set);
        }
    }

    fn zrange(&self, key: &str, range: ZRange) -> Option::<Vec::<ScoreValue>> {
        let mut res = Vec::new();

        let mut db = self.get_db(key).lock().unwrap();
        if let Some(set) = db.get(key) {        
            for v in set.iter() {
                if v.score >= range.min_score && v.score <= range.max_score {
                    res.push(ScoreValue {
                        score: v.score,
                        value: v.value.to_owned(),
                    });
                }
            }
        } else {
            return None;
        }

        Some(res)
    }

    fn zrmv(&self, key: &str, value: &str) {
        let mut db = self.get_db(key).lock().unwrap();
        if let Some(set) = db.get_mut(key) {
            let mut new_set = sorted_vec::SortedSet::with_capacity(set.capacity());
            for (index, v) in set.iter().enumerate() {
                if v.value != value {
                    new_set.find_or_insert(v.clone());
                }
            }
            db.push(key.to_string(), new_set);
        }
    }

    fn remove(&self, key: &str) {
        let mut db = self.get_db(key).lock().unwrap();
        db.pop(key);
    }
}

