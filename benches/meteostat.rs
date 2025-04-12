use criterion::{black_box, criterion_group, criterion_main, Criterion};
use meteostat::get_hourly::get_hourly;

fn bench_exiftool(c: &mut Criterion) {
    c.bench_function("spawn & read", |b| b.iter(|| get_hourly(black_box("10637"))));
}

criterion_group!(benches, bench_exiftool);
criterion_main!(benches);
