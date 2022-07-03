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

pub struct RocksdbUtil {
    pub db: rocksdb::DB
}

impl RocksdbUtil {
    pub fn set(&self, _key: &str, _value: &str) {
        self.db.put(_key, _value).unwrap();
    }

    pub fn get(&self, _key: &str) -> String {
       let s = self.db.get(_key).unwrap().unwrap();
       let q = String::from_utf8(s).unwrap();
       return q;
    }

    pub fn remove(&self, _key: &str) {
        self.db.delete(_key).unwrap();
    }

    pub fn mget(&self, _keys: &Vec<&str>) -> Vec<String> {
        let mut mock_res = Vec::new();
        let resp = self.db.multi_get(_keys);
        for v in resp {
            let val = v.unwrap().unwrap();
            let l = String::from_utf8(val).unwrap();
            mock_res.push(l);
        }
        mock_res
    }

    pub fn mset(&self, _keys: &Vec<&str>, _vals: &Vec<&str>) {
        let mut i = 0;
        while i < _keys.len() {
            self.db.put(_keys[i], _vals[i]).unwrap();
            i = i + 1;
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