fn main() {
    println!("cargo:rustc-link-search=libsql/.libs");
    println!("cargo:rustc-link-lib=static=sqlite3");
}
