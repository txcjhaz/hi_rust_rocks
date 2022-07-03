pub trait KvUtil {
    fn set(&self, key: &str, value: &str);
    fn get(&self, key: &str) -> &str;
    fn remove(&self, key: &str);
    fn mget(&self, keys:  Vec<&str>) -> Vec<&str>;
    fn mset(&self, keys: Vec<&str>, vals: Vec<&str>);
    fn zadd(&self, key: &str, value: &str, score: &u32);
    fn zrange(&self, key: &str) -> (&u32, &u32);
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

    fn mget(&self, _keys: Vec<&str>) -> Vec<&str> {
        let mut mock_res = Vec::new();
        mock_res.push("value");
        mock_res
    }

    fn mset(&self, _key: Vec<&str>, _vals: Vec<&str>) {
        "get success";
    }

    fn zadd(&self, _key: &str, _val: &str, _score: &u32) {
        "get success";
    }

    fn zrange(&self, _key: &str) -> (&u32, &u32) {
        (&1, &2)
    }

    fn zrmv(&self, _key: &str, _val: &str) {
        "get success";
    }
}