use std::cell::RefCell;
use std::f32::consts::PI;
use std::rc::Rc;
use std::simd::Simd;

use arrayvec::ArrayVec;

type V4 = Simd<f32, 4>;
type CacheKey = (usize, usize);
type CosAxisCache = ArrayVec<(CacheKey, Rc<[f32]>), 32>;
type CosAxisSimd4Cache = ArrayVec<(CacheKey, Rc<[V4]>), 32>;

pub(crate) fn precompute_cos_axis(len: usize, components: usize) -> Vec<f32> {
    let len_f = len as f32;
    let mut out = vec![0.0f32; len * components];
    for p in 0..len {
        let p_f = p as f32;
        let row = &mut out[p * components..(p + 1) * components];
        for (c, slot) in row.iter_mut().enumerate() {
            *slot = (PI * p_f * (c as f32) / len_f).cos();
        }
    }
    out
}

thread_local! {
    // Small caches: typical callers only ever use a handful of image sizes and
    // component counts. A linear scan over a tiny Vec has better locality than
    // a HashMap and avoids hashing overhead.
    static COS_AXIS_CACHE: RefCell<CosAxisCache> = RefCell::new(ArrayVec::new());
    static COS_AXIS_SIMD4_CACHE: RefCell<CosAxisSimd4Cache> = RefCell::new(ArrayVec::new());
}

pub(crate) fn cos_axis_cached(len: usize, components: usize) -> Rc<[f32]> {
    let key = (len, components);
    COS_AXIS_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        for (k, v) in cache.iter() {
            if *k == key {
                return v.clone();
            }
        }

        // Simple eviction to avoid unbounded growth in pathological cases.
        if cache.is_full() {
            cache.clear();
        }

        let rc: Rc<[f32]> = precompute_cos_axis(len, components).into();
        cache.push((key, rc.clone()));
        rc
    })
}

pub(crate) fn cos_axis_simd4_cached(len: usize, components: usize) -> Rc<[V4]> {
    let key = (len, components);
    COS_AXIS_SIMD4_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        for (k, v) in cache.iter() {
            if *k == key {
                return v.clone();
            }
        }

        // Simple eviction to avoid unbounded growth in pathological cases.
        if cache.is_full() {
            cache.clear();
        }

        let blocks = components.div_ceil(4);
        let len_f = len as f32;

        let mut out = Vec::with_capacity(len * blocks);
        for p in 0..len {
            let p_f = p as f32;
            for block in 0..blocks {
                let start = block * 4;
                let lanes = (components - start).min(4);
                let mut v = [0.0f32; 4];
                for (lane, slot) in v.iter_mut().enumerate().take(lanes) {
                    let c = start + lane;
                    *slot = (PI * p_f * (c as f32) / len_f).cos();
                }
                out.push(Simd::from_array(v));
            }
        }

        let rc: Rc<[V4]> = out.into();
        cache.push((key, rc.clone()));
        rc
    })
}
