use std::time::Instant;
use terminus_store::store::sync::*;
use terminus_store::*;

const N: usize = 10_000_000;

fn main() {
    let store = open_sync_memory_store();
    let builder = store.create_base_layer().unwrap();

    // 10M node triples: 1000 subjects x predicate "p" x 10000 objects
    for i in 0..1000 {
        let s = format!("s{}", i);
        for j in 0..10_000 {
            builder
                .add_value_triple(ValueTriple::new_node(&s, "p", &format!("o{}", j)))
                .unwrap();
        }
    }
    let layer = builder.commit().unwrap();
    assert_eq!(N, layer.triple_count());

    let s0 = layer.subject_id("s0").unwrap();
    let p = layer.predicate_id("p").unwrap();

    // (s, p, None): 1000 matches
    let t = Instant::now();
    let n1 = layer.triples_sp(s0, p).count();
    let d_iter_sp = t.elapsed();
    let t = Instant::now();
    let n2 = layer.count_triples(Some(s0), Some(p), None);
    let d_idx_sp = t.elapsed();

    // (None, p, None): all 100k match — worst case for iteration
    let t = Instant::now();
    let n3 = layer.triples_p(p).count();
    let d_iter_p = t.elapsed();
    let t = Instant::now();
    let n4 = layer.count_triples(None, Some(p), None);
    let d_idx_p = t.elapsed();

    // (None, None, None): all 100k — iterator would be triples() full scan
    let t = Instant::now();
    let n5 = layer.triples().count();
    let d_iter_all = t.elapsed();
    let t = Instant::now();
    let n6 = layer.count_triples(None, None, None);
    let d_idx_all = t.elapsed();

    println!("(s,p,_):   iterate {:>8?} ({})  indexed {:>8?} ({})", d_iter_sp, n1, d_idx_sp, n2);
    println!("(_,p,_):   iterate {:>8?} ({})  indexed {:>8?} ({})", d_iter_p, n3, d_idx_p, n4);
    println!("(_,_,_):   iterate {:>8?} ({})  indexed {:>8?} ({})", d_iter_all, n5, d_idx_all, n6);
}
