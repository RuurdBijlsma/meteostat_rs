use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meteostat::get_hourly::get_hourly;
use meteostat::get_hourly_lazy::get_hourly_lazy;
use meteostat::get_stations::StationCache;
use tokio::runtime::Runtime;

fn bench(c: &mut Criterion) {
    c.bench_function("get_hourly_lazy", |b| {
        b.iter(|| get_hourly_lazy(black_box("10637")))
    });
    c.bench_function("get_hourly", |b| b.iter(|| get_hourly(black_box("10637"))));

    let rt = Runtime::new().unwrap();

    c.bench_function("StationCache::init", |b| {
        b.iter(|| {
            rt.block_on(async {
                StationCache::init().await.unwrap();
            });
        });
    });

    let station_cache = rt.block_on(async { StationCache::init().await.unwrap() });
    c.bench_function("station_cache.query", |b| {
        b.iter(|| {
            rt.block_on(async { station_cache.query(black_box(50.), black_box(5.), black_box(5)) });
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
