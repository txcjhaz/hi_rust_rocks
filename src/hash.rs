use std::hash::Hasher;

pub struct BdkrHasher {}

// impl HashBuilder for BdkrHasher {
    
// }

// impl Hasher for BdkrHasher {
//     fn finish(&self) -> u64 {
//         todo!()
//     }

//     fn write(&mut self, bytes: &[u8]) {
//         todo!()
//     }
//     // fn write(&mut self, bytes: &[u8]) {
//     //     let BdkrHasher(mut hash) = *self;
//     //     for byte in bytes {
//     //         hash = hash ^ (*byte as u64);
//     //         hash = hash * 0x100000001b3;
//     //     }
//     //     *self = BdkrHasher(hash);
//     // }
//     // fn finish(&self) -> u64 { self.0 }
// }