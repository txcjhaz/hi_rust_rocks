use std::{io, fmt::Write};

use may_minihttp::{HttpService, HttpServiceFactory, Request, Response, KvUtil, MockKvUtil, RocksdbUtil};
use rocksdb::DB;
use serde::{Deserialize, Serialize};

use lazy_static::lazy_static;

extern crate serde;

// #[derive(Serialize)]
// struct HeloMessage {
//     message: &'static str,
// }

struct Techempower {}

#[derive(Deserialize, Serialize, Debug)]
struct KeyValue<'a> {
    key: &'a str,
    value: &'a str
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
            let val = ROCKS.get(key);
            let b = rsp.body_mut();
            b.write_str(&val).unwrap(); // TODO err handle
            // println!("key is {}, val is {}", key, val);
            rsp.header("Content-Type: text/plain");
        }
        else if req.path() == "/add" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());
            let kv: KeyValue = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常         
            ROCKS.set(kv.key, kv.value);
            // println!("to add key is {}, value is {}", kv.key, kv.value);
        }
        else if req.path().starts_with("/del/") {
            let key = &req.path()[7..];
            ROCKS.remove(key);
            // println!("del key is {}", key);
        }
        else if req.path() == "/list" {
            let r_body = req.body_();
            let keys: Vec<&str> = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常         
            
            let vals = ROCKS.mget(&keys);

            let mut resp = Vec::<KeyValue>::new();
            let mut i = 0;
            while i < vals.len() {
                let item = KeyValue {
                    key: keys[i],
                    value: &vals[i]
                };
                resp.push(item);
                i = i + 1;
            }
            let resp_body = serde_json::to_string(&resp).unwrap();
            let b = rsp.body_mut();
            b.write_str(resp_body.as_str()).unwrap(); // TODO err handle

            rsp.header("Content-Type: application/json");
        }
        else if req.path() == "/batch" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());

            let kv: Vec<KeyValue> = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常         
            let mut keys = Vec::new();
            let mut vals = Vec::new();
            for p in kv.iter() {
                keys.push(p.key);
                vals.push(p.value);
            }

            ROCKS.mset(&keys, &vals);
        }
        else if req.path().starts_with("/zadd/") {
            let key = &req.path()[6..];
            let r_body = req.body_();
            // println!("key is {}, body is {}", key, std::str::from_utf8(&r_body.to_vec()).unwrap());
            let z_val: ZValue = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常

            ROCKS.zadd(key, z_val.value, &z_val.score);
        }
        else if req.path().starts_with("/zrange/") {
            let key = &req.path()[8..];
            let r_body = req.body_();
            println!("key is {}, body is {}", key, std::str::from_utf8(&r_body.to_vec()).unwrap());
            let z_score: ZRangeScore = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常

            ROCKS.zrange(key, &z_score.min_score, &&z_score.max_score);
        }
        else if req.path().starts_with("/zrmv/") {
            let keyAndValue = &req.path()[6..];
            let splits: Vec<&str> = keyAndValue.split('/').collect();
            // println!("key is {}, val is {}", splits[0], splits[1]);
            ROCKS.zrmv(splits[0], splits[1]);
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
    ROCKS.set("test", "666");
    may::config()
        .set_pool_capacity(10000)
        .set_stack_size(0x1000);
    let http_server = HttpServer {};
    let server = http_server.start("0.0.0.0:8081").unwrap();
    server.join().unwrap();
}

lazy_static!{
    static ref ROCKS: RocksdbUtil = {
        println!("rocksdb init");
        let db = DB::open_default("C:/Users/txcjh/Desktop/Projects/may_minihttp/storage").unwrap();
        println!("rocksdb init successfully");
        RocksdbUtil { db: db }
    };
}