fn main() {
    println!("cargo:rustc-link-search=.");
    println!("cargo:rustc-link-lib=static=embeddings");
}
