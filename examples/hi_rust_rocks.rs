use std::{io, fmt::Write, collections::{HashMap, hash_map::RandomState}, sync::{Arc, Mutex}};

use may_minihttp::{HttpService, HttpServiceFactory, Request, Response, SelfKvUtil};
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use lazy_static::lazy_static;
use core::hash::BuildHasher;


extern crate serde;

const TOTAL_SLOTS: usize = 1 << 5;
const SLOT_SIZE: usize = 20000;

struct Techempower {}

#[derive(Deserialize, Serialize, Debug)]
struct KeyValue<'a> {
    key: &'a str,
    value: &'a str
}

#[derive(Deserialize, Serialize, Debug)]

struct KeyValueRes {
    key: String,
    value: String
}

#[derive(Deserialize, Serialize, Debug)]
struct ZValue<'a> {
    score: u32,
    value: &'a str
}

#[derive(Deserialize, Serialize, Debug)]
struct ZRangeScore {
    min_score: u32,
    max_score: u32
}



impl HttpService for Techempower {

    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        // Bare-bones router
        if req.path() == "/init" {
            rsp.header("Content-Type: text/plain").body("ok");
        }
        else if req.path().starts_with("/query/") {
            let key = &req.path()[7..];
            if let Some(val) = dash_map.get(key) {
                rsp.body_mut().write_str(val.as_str()).unwrap();
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
                    let key = unsafe { String::from_utf8_unchecked(kv.key.as_bytes().to_vec()) };
                    let val = unsafe { String::from_utf8_unchecked(kv.value.as_bytes().to_vec()) };
                    dash_map.insert(key, val);
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
            dash_map.remove(key);
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
                        if let Some(val) = dash_map.get(key) {
                            final_res.push(KeyValueRes{
                                key: unsafe { String::from_utf8_unchecked(key.as_bytes().to_vec()) },
                                value: unsafe { String::from_utf8_unchecked(val.as_bytes().to_vec()) }
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
                        let key = unsafe { String::from_utf8_unchecked(kv.key.as_bytes().to_vec()) };
                        let val = unsafe { String::from_utf8_unchecked(kv.value.as_bytes().to_vec()) };
                        dash_map.insert(key, val);
                    }
                },
                Err(_err) => {
                    rsp.status_code("400", "");
                    return Ok(());
                }
            }
        }
        else if req.path().starts_with("/zadd/") {
        }
        else if req.path().starts_with("/zrange/") {
        }
        else if req.path().starts_with("/zrmv/") {
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

    dash_map.insert("llll".to_string(), "我是第一个卖报的小画家".to_string());

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



lazy_static!{
    static ref KVs: SelfKvUtil = {
        let mut kv_wrap = Vec::<Arc<Mutex<HashMap<String, String>>>>::with_capacity(TOTAL_SLOTS);
        for i in 0..TOTAL_SLOTS {
            kv_wrap.push(Arc::new(Mutex::new( HashMap::<String, String>::with_capacity(SLOT_SIZE))));
        }
        let kvs = SelfKvUtil { dbs: kv_wrap };

        kvs
    };

    static ref KV: Mutex<HashMap<String, String>> = {
        Mutex::new(HashMap::with_capacity(TOTAL_SLOTS * SLOT_SIZE))
    };

    static ref unlock_kv: DashMap<String, String> = {
        DashMap::with_capacity(TOTAL_SLOTS * SLOT_SIZE)
    };

    
    static ref dash_map: DashMap<String, String, fnv::FnvBuildHasher> = {
        DashMap::<String, String, fnv::FnvBuildHasher>::with_capacity_and_hasher_and_shard_amount(14<<30/100/TOTAL_SLOTS, fnv::FnvBuildHasher::default(), TOTAL_SLOTS)
    };

}


struct SelfBuilder{
    
}
impl BuildHasher for SelfBuilder{
    type Hasher = fnv::FnvHasher;
    fn build_hasher(&self) -> Self::Hasher {
        // todo!()
        fnv::FnvHasher::default()
    }
}
impl Clone for SelfBuilder {
    fn clone(&self) -> Self {
        Self {  }
    }
}
