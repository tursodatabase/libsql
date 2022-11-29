fn main() {
    println!("cargo:rustc-link-search=native=libsql/.libs");
    println!("cargo:rustc-link-lib=static=sqlite3");
}
