use std::{io, fmt::Write, os::raw};
use bytes::BufMut;

use may_minihttp::{HttpService, HttpServiceFactory, Request, Response, KvUtil, MockKvUtil, RocksdbUtil};
use rocksdb::{DB, DBCompressionType};
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
    value: &'a str,
}

#[derive(Deserialize, Serialize, Debug)]
struct ZValue<'a> {
    score: u32,
    value: &'a str,
}

#[derive(Deserialize, Serialize, Debug)]
struct ZRangeScore {
    min_score: u32,
    max_score: u32,
}


impl HttpService for Techempower {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        // Bare-bones router
        if req.path() == "/init" {
            rsp.header("Content-Type: text/plain").body("ok");
        } else if req.path().starts_with("/query/") {
            let key = &req.path()[7..];
            let raw_val = ROCKS.get(key).unwrap();
            if (raw_val.is_none()) {
                rsp.status_code("404", "Not Found");
                return Ok(());
            }
            let val = String::from_utf8(raw_val.unwrap()).unwrap();
            rsp.body_mut().write_str(&val); // TODO err handle
            // println!("key is {}, val is {}", key, val);
            rsp.header("Content-Type: text/plain");
        } else if req.path() == "/add" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());
            let kv: KeyValue = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常         
            ROCKS.put(kv.key, kv.value);
            // println!("to add key is {}, value is {}", kv.key, kv.value);
        } else if req.path().starts_with("/del/") {
            let key = &req.path()[7..];
            ROCKS.delete(key);
            // println!("del key is {}", key);
        } else if req.path() == "/list" {
            let r_body = req.body_();
            let keys: Vec<&str> = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常         

            let b = rsp.body_mut();
            let mut i = 0;
            let len = keys.len();
            if len > 0 {
                b.write_char('[').unwrap();
            }

            let res = ROCKS.multi_get(keys.clone());

            while i < len {
                let key = keys[i];
                let raw_val = res[i].clone().unwrap();

                if (raw_val.is_none()) {
                    rsp.status_code("404", "Not Found");
                    return Ok(());
                }

                let v = raw_val.unwrap();
                let value = std::str::from_utf8(v.as_ref()).unwrap();
                let item = format!("{{\"key\":{}, \"value\":{}}}", key, value);
                b.write_str(&item).unwrap();

                i = i + 1;

                if i == keys.len() {
                    b.write_char(']').unwrap();
                } else {
                    b.write_char(',').unwrap();
                }
            }

            if len > 0 {
                b.write_char(']').unwrap();
            }
            rsp.header("Content-Type: application/json");
        } else if req.path() == "/batch" {
            let r_body = req.body_();
            // println!("body is {}", std::str::from_utf8(&r_body.to_vec()).unwrap());

            let kv: Vec<KeyValue> = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常         
            for p in kv.iter() {
                ROCKS.put(p.key, p.value).unwrap();
            }
        } else if req.path().starts_with("/zadd/") {
            // let key = &req.path()[6..];
            // let r_body = req.body_();
            // // println!("key is {}, body is {}", key, std::str::from_utf8(&r_body.to_vec()).unwrap());
            // let z_val: ZValue = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常
            //
            // ROCKS.zadd(key, z_val.value, &z_val.score);
        } else if req.path().starts_with("/zrange/") {
            // let key = &req.path()[8..];
            // let r_body = req.body_();
            // println!("key is {}, body is {}", key, std::str::from_utf8(&r_body.to_vec()).unwrap());
            // let z_score: ZRangeScore = serde_json::from_slice(r_body).unwrap();  // FIXME 处理异常
            //
            // ROCKS.zrange(key, &z_score.min_score, &&z_score.max_score);
        } else if req.path().starts_with("/zrmv/") {
            // let keyAndValue = &req.path()[6..];
            // let splits: Vec<&str> = keyAndValue.split('/').collect();
            // // println!("key is {}, val is {}", splits[0], splits[1]);
            // ROCKS.zrmv(splits[0], splits[1]);
        } else {
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
    ROCKS.put("test", "666");
    may::config()
        .set_pool_capacity(10000)
        .set_stack_size(0x1000);
    let http_server = HttpServer {};
    let server = http_server.start("0.0.0.0:8080").unwrap();
    server.join().unwrap();
}

lazy_static! {
    static ref ROCKS: rocksdb::DB = {
        println!("rocksdb init");

        let mut cf_opts = rocksdb::Options::default();
        cf_opts.set_max_write_buffer_number(16);
        cf_opts.set_allow_mmap_writes(true);
        cf_opts.set_allow_mmap_reads(true);
        cf_opts.set_write_buffer_size(512 << 20);
        cf_opts.set_compression_type(DBCompressionType::Lz4);
        cf_opts.set_bottommost_compression_type(DBCompressionType::Zstd);
        cf_opts.set_level_compaction_dynamic_level_bytes(true);
        let cf = rocksdb::ColumnFamilyDescriptor::new("cf1", cf_opts);
        
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_db_write_buffer_size(4 << 20);
        db_opts.set_optimize_filters_for_hits(true);
        // db_opts.set_ratelimiter(1024 * 1024, 100 * 1000, 10);
        db_opts.set_max_background_jobs(4);
        db_opts.set_bytes_per_sync(1048576);

        // enable block cache
        let cache = rocksdb::Cache::new_lru_cache(6 << 30).unwrap();
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        block_opts.set_block_size(16 * 1024);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_opts.set_format_version(5);
        // enable bloom filter optimize
        block_opts.set_bloom_filter(10.0, false);

        db_opts.set_block_based_table_factory(&block_opts);


        // let db = DB::open_default("/data/").unwrap();
        let db = DB::open_cf_descriptors(&db_opts, "/data/", vec![cf]).unwrap();
        println!("rocksdb init successfully");
        db
    };
}