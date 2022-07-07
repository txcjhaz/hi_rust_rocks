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
                    let mut resp = Vec::<String>::new();
                    
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

                    for (i, val_resp) in vals.iter().enumerate() {
                        match val_resp {
                            Ok(maybe_val) => {
                                match maybe_val {
                                    Some(val) => {
                                        let val_str = std::str::from_utf8(val).unwrap();
                                        let kv_str = format!("{{\"key\":\"{}\",\"value\":\"{}\"}}", keys[i], val_str);
                                        
                                        resp.push(kv_str);
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

                    let json_str_resp = serde_json::to_string(&resp);
                    match json_str_resp {
                        Ok(s) => {
                            b.write_str(&s).unwrap();
                            rsp.header("Content-Type: application/json");
                            return Ok(());
                        },
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
            return Ok(());
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
        .set_stack_size(0x1000)
        .set_workers(1);
    let http_server = HttpServer {};
    let server = http_server.start("0.0.0.0:8080").unwrap();
    server.join().unwrap();
}

lazy_static!{
    static ref ROCKS: rocksdb::DB = {
        println!("rocksdb init");

        let mut db_opts = rocksdb::Options::default();

        // use direct io (cannot use with mmap at the same time)
        db_opts.set_use_direct_reads(true);
        db_opts.set_use_direct_io_for_flush_and_compaction(true);
        db_opts.set_compaction_readahead_size(2 << 20);
        db_opts.set_writable_file_max_buffer_size(1 << 20);

        // block cache setting
        let mut block_cache_opts = rocksdb::BlockBasedOptions::default();
        let lru_cache = rocksdb::Cache::new_lru_cache(5 << 30).unwrap();
        block_cache_opts.set_block_cache(&lru_cache);
        block_cache_opts.set_bloom_filter(10.0, false);
        block_cache_opts.set_block_size(16 << 10);
        block_cache_opts.set_cache_index_and_filter_blocks(true);
        block_cache_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_cache_opts.set_format_version(5);
        db_opts.set_block_based_table_factory(&block_cache_opts);

        // rate limiter setting, usually change first param
        db_opts.set_ratelimiter(1 << 20, 100 * 1000, 10);

        // general opt
        db_opts.set_max_background_jobs(2);
        db_opts.set_bytes_per_sync(1 << 20);

        // wal setting
        db_opts.set_wal_bytes_per_sync(1 << 20);

        let db = DB::open_default("/data").unwrap();
        println!("rocksdb init successfully");
        db
    };
}