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
            let f = ROCKS.get(key);

            match f {
                Ok(val) => {
                    match val {
                        Some(v) => {
                            let str = std::str::from_utf8(&v);
                            match str {
                                Ok(s) => {
                                    rsp.body_mut().write_str(s).unwrap();
                                    rsp.header("Content-Type: text/plain");
                                },
                                Err(err) => {
                                    rsp.status_code("404", "");
                                    return Ok(());
                                }
                            }
                        },
                        None => {
                            rsp.status_code("404", "");
                            return Ok(());
                        }
                    };
                },
                Err(_err) => {
                    rsp.status_code("404", "");
                    return Ok(());
                }
            }
        }
        else if req.path() == "/add" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());
            let json_parse_resp: Result<KeyValue, serde_json::Error> = serde_json::from_slice(r_body); 
            match json_parse_resp {
                Ok(kv) => {
                    let f = ROCKS.put(kv.key, kv.value);
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
            // println!("del key is {}", key);
            let f = ROCKS.delete(key);
            match f {
                Ok(()) => {},
                Err(_err) => {
                    rsp.status_code("400", "");
                    return Ok(());
                }
            }
            // println!("del key is {}", key);
        }
        else if req.path() == "/list" {
            let r_body = req.body_();
            let json_parse_resp: Result<Vec<&str>, serde_json::Error> = 
                serde_json::from_slice(r_body);
            
            let b = rsp.body_mut();
            match json_parse_resp {
                Ok(keys) => {
                    let vals = ROCKS.multi_get(keys.clone());
                    
                    if vals.len() == 0 {
                        let f = b.write_str("[]");
                        match f {
                            Ok(()) => {},
                            Err(_err) => {
                                rsp.status_code("404", "");
                                return Ok(());
                            }
                        }
                        return Ok(());
                    }

                    let f = b.write_char('[');
                    match f {
                        Ok(()) => {},
                        Err(_err) => {
                            rsp.status_code("404", "");
                            return Ok(());
                        }
                    }
                    
                    let len = keys.len() - 1;
                    for (i, val_resp) in vals.iter().enumerate() {
                        match val_resp {
                            Ok(maybe_val) => {
                                match maybe_val {
                                    Some(val) => {
                                        let val_str = std::str::from_utf8(val).unwrap();
                                        let kv_str = format!("{{\"key\":\"{}\",\"value\":\"{}\"}}", keys[i], val_str);
                                        
                                        let f = b.write_str(&kv_str);

                                        match f {
                                            Ok(()) => {},
                                            Err(_err) => {
                                                rsp.status_code("404", "");
                                                return Ok(());
                                            }
                                        }

                                        if i < len  {
                                            let f = b.write_char(',');
                                            match f {
                                                Ok(()) => {},
                                                Err(_err) => {
                                                    rsp.status_code("404", "");
                                                    return Ok(());
                                                }
                                            }
                                        }

                                    },
                                    None => {
                                        rsp.status_code("404", "");
                                        return Ok(());
                                    }
                                }
                            },
                            Err(_err) => {
                                rsp.status_code("404", "");
                                return Ok(());
                            }
                        }
                    }

                    let f = b.write_char(']');
                    match f {
                        Ok(()) => {},
                        Err(_err) => {
                            rsp.status_code("404", "");
                            return Ok(());
                        }
                    }
                },
                Err(_err) => {
                    rsp.status_code("404", "");
                    return Ok(());
                }
            }

            rsp.header("Content-Type: application/json");
        }
        else if req.path() == "/batch" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());

            let kv_parse_rsp: Result<Vec<KeyValue>, serde_json::Error> = serde_json::from_slice(r_body); 
            match kv_parse_rsp {
                Ok(kvs) => {
                    for kv in kvs {
                        let f = ROCKS.put(kv.key, kv.value);
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
    ROCKS.put("test", "666").unwrap();
    may::config()
        .set_pool_capacity(10000)
        .set_stack_size(0x1000);
    let http_server = HttpServer {};
    let server = http_server.start("0.0.0.0:8080").unwrap();
    server.join().unwrap();
}

lazy_static!{
    static ref ROCKS: rocksdb::DB = {
        println!("rocksdb init");
        let db = DB::open_default("data").unwrap();
        println!("rocksdb init successfully");
        db
    };
}