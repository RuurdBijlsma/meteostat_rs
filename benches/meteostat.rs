use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meteostat::get_hourly::get_hourly;
use meteostat::get_hourly_lazy::get_hourly_lazy;

fn bench_exiftool(c: &mut Criterion) {
    c.bench_function("get_hourly_lazy", |b| b.iter(|| get_hourly_lazy(black_box("10637"))));
    c.bench_function("get_hourly", |b| b.iter(|| get_hourly(black_box("10637"))));
}

criterion_group!(benches, bench_exiftool);
criterion_main!(benches);
