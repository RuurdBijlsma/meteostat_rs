use chrono::{DateTime, NaiveDate, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meteostat::get_stations::StationCache;
use tokio::runtime::Runtime;

fn bench(c: &mut Criterion) {
    let naive = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let utc = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
    let station_str = "10637";

    let rt = Runtime::new().unwrap();

    c.bench_function("[gm] get_hourly_frame", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _l = meteostat::get_hourly::get_hourly_lazy(black_box(station_str)).await.unwrap();
            });
        });
    });

    let lf = rt.block_on(async { meteostat::get_hourly::get_hourly_lazy(station_str).await.unwrap() });

    c.bench_function("[gm] get_hourly_from_df", |b| {
        b.iter(|| {
            rt.block_on(async {
                meteostat::get_hourly::get_hourly_from_df(black_box(lf.clone()), black_box(utc)).unwrap();
            });
        });
    });

    c.bench_function("[gm] get_hourly_from_station", |b| {
        b.iter(|| {
            rt.block_on(async {
                meteostat::get_hourly::get_hourly_from_station(black_box(station_str), black_box(utc))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("[lazy] get_hourly_frame", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _l = meteostat::get_hourly::get_hourly_lazy(black_box(station_str)).await.unwrap();
            });
        });
    });

    let lf = rt.block_on(async { meteostat::get_hourly::get_hourly_lazy(station_str).await.unwrap() });

    c.bench_function("[lazy] get_hourly_from_df", |b| {
        b.iter(|| {
            rt.block_on(async {
                meteostat::get_hourly::get_hourly_from_df(black_box(lf.clone()), black_box(utc)).unwrap();
            });
        });
    });

    c.bench_function("[lazy] get_hourly_from_station", |b| {
        b.iter(|| {
            rt.block_on(async {
                meteostat::get_hourly::get_hourly_from_station(black_box(station_str), black_box(utc))
                    .await
                    .unwrap();
            });
        });
    });

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
