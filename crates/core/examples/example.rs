use libsql_core::Database;

fn main() {
    let db = Database::open(":memory:");
    let con = db.connect().unwrap();
}
