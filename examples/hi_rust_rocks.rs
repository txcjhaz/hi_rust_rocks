use std::{io, fmt::Write};

use may_minihttp::{HttpService, HttpServiceFactory, Request, Response, KvUtil, MockKvUtil};
use rocksdb::DB;
use serde::Deserialize;

extern crate serde;

// #[derive(Serialize)]
// struct HeloMessage {
//     message: &'static str,
// }

struct Techempower {
    kv: MockKvUtil
}

#[derive(Deserialize, Debug)]
struct KeyValue<'a> {
    key: &'a str,
    value: &'a str
}

impl HttpService for Techempower {

    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        // Bare-bones router
        if req.path() == "/init" {
            rsp.header("Content-Type: text/plain").body("ok");
        }
        else if req.path().starts_with("/query/") {
            let key = &req.path()[7..];
            let val = self.kv.get(key);
            let b = rsp.body_mut();
            b.write_str(val).unwrap(); // TODO err handle
            // println!("key is {}, val is {}", key, val);
            rsp.header("Content-Type: text/plain");
        }
        else if req.path() == "/add" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());
            let kv: KeyValue = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常         
            self.kv.set(kv.key, kv.value);
            // println!("to add key is {}, value is {}", kv.key, kv.value);
        }
        else if req.path().starts_with("/del/") {
            let key = &req.path()[7..];
            self.kv.remove(key);
            // println!("del key is {}", key);
        }
        else if req.path() == "/list" {
            let r_body = req.body_();
            let keys: Vec<&str> = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常         
            let vals = self.kv.mget(keys);
            let mut resp = Vec::<KeyValue>::new();
            let mut i = 0;
            // while i < vals.len() {
            //     let item = KeyValue {
            //         key: keys[i].clone(),
            //         value: vals[i].clone()
            //     };
            //     resp.push(item);
            //     i = i + 1;
            // }
            
            rsp.header("Content-Type: text/plain").body("get response");
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

            self.kv.mset(keys, vals);
        }
        else if req.path().starts_with("/zadd/") {
            rsp.header("Content-Type: text/plain").body("get response");
        }
        else if req.path().starts_with("/zrange/") {
            rsp.header("Content-Type: text/plain").body("get response");
        }
        else if req.path().starts_with("/zrmv/") {
            rsp.header("Content-Type: text/plain").body("get response");
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
        let kv_util_impl = MockKvUtil {};
        Techempower { kv: kv_util_impl }
    }
}

fn main() {
    // let mut db = DB::open_default("/path/for/rocksdb/storage").unwrap();
    // db.put(b"my key", b"my value");
    may::config()
        .set_pool_capacity(10000)
        .set_stack_size(0x1000);
    let http_server = HttpServer {};
    let server = http_server.start("0.0.0.0:8081").unwrap();
    server.join().unwrap();
}