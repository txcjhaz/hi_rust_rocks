use std::{io, fmt::Write, hash::Hash, collections::{hash_map::DefaultHasher, HashMap}, sync::{Arc, Mutex}};

use may_minihttp::{HttpService, HttpServiceFactory, Request, Response, KvUtil, MockKvUtil, SelfKvUtil};
use serde::{Deserialize, Serialize};

use lazy_static::lazy_static;

extern crate serde;

const TOTAL_SLOTS: usize = 1024;
const SLOT_SIZE: usize = 20000;

struct Techempower {}

#[derive(Deserialize, Serialize, Debug)]
struct KeyValue {
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
            if let Some(val) = KVs.get(key) {
                rsp.body_mut().write_str(&val).unwrap();
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
                    let f = KVs.set(&kv.key, &kv.value);
                    match f {
                        Ok(()) => {},
                        Err(_err) => {
                            rsp.status_code("400", "");
                            return Ok(());
                        }
                    }
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
            KVs.remove(key);
            // println!("del key is {}", key);
        }
        else if req.path() == "/list" {
            let r_body = req.body_();
            let json_parse_resp: Result<Vec<&str>, serde_json::Error> = 
                serde_json::from_slice(r_body);
            
            let b = rsp.body_mut();
            match json_parse_resp {
                Ok(keys) => {
                    let res = KVs.mget(&keys);
                    match res {
                        Ok(vals) => {
                            let mut final_res = Vec::<KeyValue>::with_capacity(vals.len());
                            for val in vals {
                                final_res.push(KeyValue{
                                    key: val.0,
                                    value: val.1
                                });
                            }
                            let body_mut = rsp.body_mut();
                            let r = serde_json::to_string(&final_res).unwrap();
                            body_mut.write_str(&r).unwrap();
                        },
                        Err(_) => {
                            rsp.status_code("400", "");
                        }
                    }
            
                },
                Err(_err) => {
                    rsp.status_code("400", "");
                }
            }
            return Ok(());
        }
        else if req.path() == "/batch" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());

            let kv_parse_rsp: Result<Vec<KeyValue>, serde_json::Error> = serde_json::from_slice(r_body); 
            match kv_parse_rsp {
                Ok(kvs) => {
                    for kv in kvs {
                        let f = KVs.set(&kv.key, &kv.value);
                        match f {
                            Ok(()) => {},
                            Err(_err) => {
                                rsp.status_code("400", "");
                                return Ok(());
                            }
                        }
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

struct HttpServer {
}

impl HttpServiceFactory for HttpServer {
    type Service = Techempower;

    fn new_service(&self) -> Self::Service {
        // let kv_util_impl = MockKvUtil {};
        Techempower {}
    }

}

fn main() {
    may::config()
        .set_pool_capacity(10000)
        .set_stack_size(0x1000)
        .set_workers(1);
    let http_server = HttpServer {};
    let server = http_server.start("0.0.0.0:8080").unwrap();
    server.join().unwrap();
}

lazy_static!{
    static ref KVs: SelfKvUtil = {
        let mut kv_wrap = Vec::<Arc<Mutex<HashMap<String, String>>>>::with_capacity(TOTAL_SLOTS);
        for i in 0..TOTAL_SLOTS {
            kv_wrap.push(Arc::new(Mutex::new( HashMap::<String, String>::with_capacity(SLOT_SIZE))));
        }
        let kvs = SelfKvUtil { dbs: kv_wrap };

        kvs
    };
}