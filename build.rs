use std::{env, fs};

fn main() {
    let prefix_len = env::var("PREFIX_LEN").unwrap_or_else(|_| "2".to_string());
    let code = format!("pub const PREFIX_LEN: usize = {};", prefix_len);
    fs::write("src/generated.rs", code).unwrap();
}