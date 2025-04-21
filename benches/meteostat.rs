use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meteostat::meteostat::{LatLon, Meteostat};
use meteostat::stations::locate_station::StationLocator;
use meteostat::types::data_source::Frequency;
use meteostat::utils::get_cache_dir;
use tokio::runtime::Runtime;

fn bench(c: &mut Criterion) {
    let station_str = "10637";

    let rt = Runtime::new().unwrap();

    c.bench_function("meteostat::new", |b| {
        b.iter(|| {
            rt.block_on(async {
                Meteostat::new().await.unwrap();
            });
        });
    });

    let meteostat = rt.block_on(async { Meteostat::new().await.unwrap() });

    c.bench_function("meteostat.from_station.hourly", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = meteostat
                    .from_station()
                    .station(black_box(station_str))
                    .frequency(black_box(Frequency::Hourly))
                    .call()
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.hourly", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = meteostat
                    .from_location()
                    .location(black_box(LatLon {
                        lat: 50.0,
                        lon: 5.0,
                    }))
                    .frequency(black_box(Frequency::Hourly))
                    .call()
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.daily", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = meteostat
                    .from_location()
                    .location(black_box(LatLon {
                        lat: 50.038,
                        lon: 8.559,
                    }))
                    .frequency(black_box(Frequency::Daily))
                    .call()
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.monthly", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = meteostat
                    .from_location()
                    .location(black_box(LatLon {
                        lat: 50.038,
                        lon: 8.559,
                    }))
                    .frequency(black_box(Frequency::Monthly))
                    .call()
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.climate", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = meteostat
                    .from_location()
                    .location(black_box(LatLon {
                        lat: 50.038,
                        lon: 8.559,
                    }))
                    .frequency(black_box(Frequency::Climate))
                    .call()
                    .await
                    .unwrap();
            });
        });
    });

    let cache_dir = get_cache_dir().unwrap();

    c.bench_function("StationCache::init", |b| {
        b.iter(|| {
            rt.block_on(async {
                StationLocator::new(&cache_dir).await.unwrap();
            });
        });
    });

    let station_cache = rt.block_on(async { StationLocator::new(&cache_dir).await.unwrap() });
    c.bench_function("station_cache.query", |b| {
        b.iter(|| {
            rt.block_on(async {
                station_cache.query(black_box(50.), black_box(5.), black_box(5), black_box(30.0))
            });
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
