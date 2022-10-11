#[link(name="add")]
extern "C" {
    fn add(a: i32, b: i32) -> i32;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let res = unsafe {
            add(17, 25)
        };
        println!("17 + 25 = {}", res);
    }
}
