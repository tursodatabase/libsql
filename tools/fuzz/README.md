# Fuzz

install cargo afl:
`cargo install cargo-afl`

build the fuzz crate:
`cargo afl build`

run the tests:
'''
cargo afl fuzz -i dicts -x dicts/sql.dict -o out target/debug/fuzz parser
'''

for more info: <https://rust-fuzz.github.io/book/afl.html>
