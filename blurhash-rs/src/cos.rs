use std::cell::RefCell;
use std::f32::consts::PI;
use std::rc::Rc;
use std::simd::Simd;

pub fn precompute_cos_axis(len: usize, components: usize) -> Vec<f32> {
    let len_f = len as f32;
    let mut out = vec![0.0f32; len * components];
    for p in 0..len {
        let p_f = p as f32;
        let row = &mut out[p * components..(p + 1) * components];
        for c in 0..components {
            row[c] = (PI * p_f * (c as f32) / len_f).cos();
        }
    }
    out
}

thread_local! {
    // Small caches: typical callers only ever use a handful of image sizes and
    // component counts. A linear scan over a tiny Vec has better locality than
    // a HashMap and avoids hashing overhead.
    static COS_AXIS_CACHE: RefCell<Vec<((usize, usize), Rc<[f32]>)>> =
        RefCell::new(Vec::with_capacity(1));
    static COS_AXIS_SIMD4_CACHE: RefCell<Vec<((usize, usize), Rc<[Simd<f32, 4>]>)>> =
        RefCell::new(Vec::with_capacity(1));
}

pub fn cos_axis_cached(len: usize, components: usize) -> Rc<[f32]> {
    let key = (len, components);
    COS_AXIS_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        for (k, v) in cache.iter() {
            if *k == key {
                return v.clone();
            }
        }

        // Simple eviction to avoid unbounded growth in pathological cases.
        if cache.len() >= 32 {
            cache.clear();
        }

        let vec = precompute_cos_axis(len, components);
        let rc: Rc<[f32]> = vec.into();
        cache.push((key, rc.clone()));
        rc
    })
}

pub fn cos_axis_simd4_cached(len: usize, components: usize) -> Rc<[Simd<f32, 4>]> {
    let key = (len, components);
    COS_AXIS_SIMD4_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        for (k, v) in cache.iter() {
            if *k == key {
                return v.clone();
            }
        }

        // Simple eviction to avoid unbounded growth in pathological cases.
        if cache.len() >= 32 {
            cache.clear();
        }

        let blocks = (components + 3) / 4;
        let len_f = len as f32;

        let mut out = Vec::with_capacity(len * blocks);
        for p in 0..len {
            let p_f = p as f32;
            for block in 0..blocks {
                let start = block * 4;
                let lanes = (components - start).min(4);
                let mut v = [0.0f32; 4];
                for lane in 0..lanes {
                    let c = start + lane;
                    v[lane] = (PI * p_f * (c as f32) / len_f).cos();
                }
                out.push(Simd::from_array(v));
            }
        }

        let rc: Rc<[Simd<f32, 4>]> = out.into();
        cache.push((key, rc.clone()));
        rc
    })
}
