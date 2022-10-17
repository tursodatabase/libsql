fn main() {
    println!("cargo:rustc-link-search=../../.libs");
    println!("cargo:rustc-link-lib=sqlite3");
}
