set windows-shell := ["powershell.exe", "-Command"]

# --- Lints:

check: fmt clippy test

fmt:
    cargo fmt --all

clippy:
    cargo clippy --no-deps --all-features --tests --benches -- \
        -D clippy::all \
        -D clippy::pedantic \
        -D clippy::nursery

# --- Misc:

clean:
    cargo clean

# --- Execution:

test:
    cargo test -- --nocapture

bench:
    cargo bench

run:
    cargo run --example basic_use

