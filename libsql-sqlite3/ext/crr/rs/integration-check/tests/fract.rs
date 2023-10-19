use sqlite::{Connection, ResultCode};
use sqlite_nostd as sqlite;

#[test]
fn sort_no_list_col() {
    let w = integration_utils::opendb().expect("db opened");
    let db = &w.db;

    db.exec_safe("CREATE TABLE todo (id primary key, position)")
        .expect("table created");
    db.exec_safe("SELECT crsql_fract_as_ordered('todo', 'position')")
        .expect("as ordered");
    db.exec_safe(
        "INSERT INTO todo VALUES (1, 'Zm'), (2, 'ZmG'), (3, 'ZmG'), (4, 'ZmV'), (5, 'Zn')",
    )
    .expect("inserted initial values");
    db.exec_safe("UPDATE todo_fractindex SET after_id = 2 WHERE id = 5")
        .expect("repositioned id 5");
}
