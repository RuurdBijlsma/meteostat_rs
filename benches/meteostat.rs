use chrono::{NaiveDate, TimeZone, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meteostat::{Frequency, InventoryRequest, LatLon, Meteostat, RequiredData, Year};
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
                    .hourly()
                    .station(black_box(station_str))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.hourly", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = meteostat
                    .hourly()
                    .location(black_box(LatLon(50.038, 8.559)))
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
                    .daily()
                    .location(black_box(LatLon(50.038, 8.559)))
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
                    .monthly()
                    .location(black_box(LatLon(50.038, 8.559)))
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
                    .climate()
                    .location(black_box(LatLon(50.038, 8.559)))
                    .call()
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.hourly+filter+collect", |b| {
        b.iter(|| {
            rt.block_on(async {
                let start_utc = Utc.with_ymd_and_hms(2023, 10, 26, 0, 0, 0).unwrap();
                let end_utc = Utc.with_ymd_and_hms(2023, 10, 26, 23, 59, 59).unwrap();

                let lazy_frame = meteostat
                    .hourly()
                    .location(black_box(LatLon(50.038, 8.559)))
                    .call()
                    .await
                    .unwrap();

                let freq_lazy = lazy_frame.get_range(start_utc, end_utc).unwrap();
                let _ = freq_lazy.frame.collect().unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.daily+filter+collect", |b| {
        b.iter(|| {
            rt.block_on(async {
                let start_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
                let end_date = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();

                let lazy_frame = meteostat
                    .daily()
                    .location(black_box(LatLon(50.038, 8.559)))
                    .call()
                    .await
                    .unwrap();

                let filtered_lazy_frame = lazy_frame.get_range(start_date, end_date).unwrap();
                let _ = filtered_lazy_frame.frame.collect().unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.monthly+filter+collect", |b| {
        b.iter(|| {
            rt.block_on(async {
                let lazy_frame = meteostat
                    .monthly()
                    .location(black_box(LatLon(50.038, 8.559)))
                    .call()
                    .await
                    .unwrap();

                let filtered_lazy_frame = lazy_frame.get_range(Year(2020), Year(2022)).unwrap();
                let _ = filtered_lazy_frame.frame.collect().unwrap();
            });
        });
    });

    c.bench_function("meteostat.from_location.climate+filter+collect", |b| {
        b.iter(|| {
            rt.block_on(async {
                let lazy_frame = meteostat
                    .climate()
                    .location(black_box(LatLon(50.038, 8.559)))
                    .call()
                    .await
                    .unwrap();

                // Filter for the 1991-2020 climate period records specifically
                let filtered_lazy_frame = lazy_frame.get_at(Year(1991), Year(2020), 11);

                // collect() handles potential errors
                let _ = filtered_lazy_frame.frame.collect().unwrap();
            });
        });
    });

    c.bench_function("meteostat.find_stations", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _stations = meteostat
                    .find_stations()
                    .location(black_box(LatLon(50.038, 8.559)))
                    .station_limit(black_box(3))
                    .inventory_request(black_box(InventoryRequest::new(
                        Frequency::Hourly,
                        RequiredData::FullYear(2020),
                    )))
                    .call()
                    .await
                    .unwrap();
            });
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
