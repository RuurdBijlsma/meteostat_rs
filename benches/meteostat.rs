use chrono::{DateTime, NaiveDate, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meteostat::stations::locate_station::StationLocator;
use meteostat::weather_data::fetch::{
    fetch_climate_normal, fetch_daily_weather, fetch_hourly_weather, fetch_monthly_weather,
};
use tokio::runtime::Runtime;

fn bench(c: &mut Criterion) {
    let naive = NaiveDate::from_ymd_opt(2025, 1, 6)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let datetime = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
    let date = datetime.date_naive();
    let station_str = "10637";

    let rt = Runtime::new().unwrap();

    c.bench_function("[generic] fetch_hourly_weather", |b| {
        b.iter(|| {
            rt.block_on(async {
                fetch_hourly_weather(black_box(station_str), black_box(datetime))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("[generic] fetch_daily_weather", |b| {
        b.iter(|| {
            rt.block_on(async {
                fetch_daily_weather(black_box(station_str), black_box(date))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("[generic] fetch_monthly_weather", |b| {
        b.iter(|| {
            rt.block_on(async {
                fetch_monthly_weather(black_box(station_str), black_box(2020), black_box(7))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("[generic] fetch_climate_normal", |b| {
        b.iter(|| {
            rt.block_on(async {
                fetch_climate_normal(
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
                StationLocator::init().await.unwrap();
            });
        });
    });

    let station_cache = rt.block_on(async { StationLocator::init().await.unwrap() });
    c.bench_function("station_cache.query", |b| {
        b.iter(|| {
            rt.block_on(async { station_cache.query(black_box(50.), black_box(5.), black_box(5)) });
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
