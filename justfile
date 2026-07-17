set windows-shell := ["powershell.exe", "-Command"]

# --- Lints:

check: fmt clippy test

fmt:
    cargo fmt --all

clippy:
    cargo clippy -

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

