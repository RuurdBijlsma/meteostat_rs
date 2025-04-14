use chrono::{DateTime, NaiveDate, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meteostat::stations::locate_station::StationLocator;
use meteostat::utils::get_cache_dir;
use meteostat::weather_data::fetcher::WeatherFetcher;
use tokio::runtime::Runtime;

fn bench(c: &mut Criterion) {
    let naive = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let datetime = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
    let date = datetime.date_naive();
    let station_str = "10637";
    let cache_dir = get_cache_dir().unwrap();

    let rt = Runtime::new().unwrap();
    let fetcher = WeatherFetcher::new(&cache_dir);

    c.bench_function("fetcher.hourly", |b| {
        b.iter(|| {
            rt.block_on(async {
                fetcher
                    .hourly(black_box(station_str), black_box(datetime))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("fetcher.daily", |b| {
        b.iter(|| {
            rt.block_on(async {
                fetcher
                    .daily(black_box(station_str), black_box(date))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("fetcher.monthly", |b| {
        b.iter(|| {
            rt.block_on(async {
                fetcher
                    .monthly(black_box(station_str), black_box(2020), black_box(7))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("fetcher.climate_normals", |b| {
        b.iter(|| {
            rt.block_on(async {
                fetcher
                    .climate_normals(
                        black_box(station_str),
                        black_box(1991),
                        black_box(2020),
                        black_box(6),
                    )
                    .await
                    .unwrap();
            });
        });
    });

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
