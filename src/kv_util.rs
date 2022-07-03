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