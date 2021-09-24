use bytes::Bytes;
use num_bigint::BigInt;
use std::any::Any;
use uuid::Uuid;

use crate::tuple::Versionstamp;

/// TODO
#[derive(Debug)]
pub struct Tuple {
    elements: Vec<Box<dyn Any>>,
    incomplete_versionstamp: bool,
}

impl Tuple {
    /// TODO
    pub fn new() -> Tuple {
        Tuple {
            elements: Vec::new(),
            incomplete_versionstamp: false,
        }
    }

    /// TODO
    pub fn add_bool(&mut self, b: bool) {
        let boxed_b = Box::new(b);
        self.elements.push(boxed_b);
    }

    /// todo
    pub fn add_bytes(&mut self, b: Bytes) {
        let boxed_b = Box::new(b);
        self.elements.push(boxed_b);
    }

    /// todo double
    pub fn add_f64(&mut self, f: f64) {
        let boxed_f = Box::new(f);
        self.elements.push(boxed_f);
    }

    /// todo float
    pub fn add_f32(&mut self, f: f32) {
        let boxed_f = Box::new(f);
        self.elements.push(boxed_f);
    }

    /// todo long
    pub fn add_i64(&mut self, i: i64) {
        let boxed_i = Box::new(i);
        self.elements.push(boxed_i);
    }

    /// todo tuple
    pub fn add_tuple(&mut self, t: Tuple) {
        let boxed_t = Box::new(t);
        self.elements.push(boxed_t);
    }

    /// todo
    pub fn add_versionstamp(&mut self, v: Versionstamp) {
        let boxed_v = Box::new(v);
        self.elements.push(boxed_v);
    }

    /// todo
    pub fn add_string(&mut self, s: String) {
        let boxed_s = Box::new(s);
        self.elements.push(boxed_s);
    }

    /// todo
    pub fn add_bigint(&mut self, b: BigInt) {
        let boxed_b = Box::new(b);
        self.elements.push(boxed_b);
    }

    /// todo
    pub fn add_uuid(&mut self, u: Uuid) {
        let boxed_u = Box::new(u);
        self.elements.push(boxed_u);
    }

    /// todo
    ///
    /// # Panic
    ///
    /// todo
    pub fn add_any(&mut self, a: Box<dyn Any>) {
        if (&*a).is::<bool>()
            || (&*a).is::<Bytes>()
            || (&*a).is::<f64>()
            || (&*a).is::<f32>()
            || (&*a).is::<i64>()
            || (&*a).is::<Tuple>()
            || (&*a).is::<Versionstamp>()
            || (&*a).is::<String>()
            || (&*a).is::<BigInt>()
            || (&*a).is::<Uuid>()
        {
            self.elements.push(a);
        } else {
            panic!("Tried to add a value of an unsupported type to tuple.");
        }
    }

    // TODO: CONTINUE FROM unpack after understanding `nom` parser.
}
