extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;
use sqlite::{Connection, ManagedConnection, ResultCode};
use sqlite_nostd as sqlite;

fn idempotent() {
    let db = crate::opendb().expect("db opened");
    let schema = "
      CREATE TABLE IF NOT EXISTS item (id integer primary key not null, data any) strict;
      CREATE TABLE IF NOT EXISTS container (id integer primary key, contained integer);
      CREATE INDEX IF NOT EXISTS container_contained ON container (contained);
      SELECT crsql_as_crr('item');
  ";
    db.db.exec_safe(schema).expect("schema made");
    invoke_automigrate(&db.db, schema).expect("migrated");

    assert!(expect_tables(&db.db, vec!["item", "container"]).expect("compared tables"));
    assert!(
        expect_indices(&db.db, "container", vec!["container_contained"]).expect("compared indices")
    );
}

fn change_index_col_order() {}

fn add_many_cols() {}

fn remove_many_cols() {}

fn remove_indexed_cols() {}

fn add_crr() {}

fn add_table() {}

fn remove_table() {}

fn remove_crr() {}

fn primary_key_change() {}

fn with_default_value() {}

fn not_null() {}

fn nullable() {}

fn no_default_value() {}

fn strut_schema() {
    let db = crate::opendb().expect("db opened");
    let stmt = db
        .db
        .prepare_v2(
            r#"
SELECT crsql_automigrate(?, 'SELECT crsql_finalize();')"#,
        )
        .expect("migrate statement prepared");
    stmt.bind_text(
        1,
        r#"
CREATE TABLE IF NOT EXISTS "deck" (
"id" INTEGER primary key not null,
"title",
"created",
"modified",
"theme_id",
"chosen_presenter"
);

CREATE TABLE IF NOT EXISTS "slide" (
"id" INTEGER primary key not null,
"deck_id",
"order",
"created",
"modified",
"x",
"y",
"z"
);

CREATE INDEX IF NOT EXISTS "slide_deck_id" ON "slide" ("deck_id", "order");

CREATE TABLE IF NOT EXISTS "text_component" (
"id" INTEGER primary key not null,
"slide_id",
"text",
"styles",
"x",
"y"
);

CREATE TABLE IF NOT EXISTS "embed_component" ("id" primary key not null, "slide_id", "src", "x", "y");

CREATE INDEX IF NOT EXISTS "embed_component_slide_id" ON "embed_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "shape_component" (
"id" INTEGER primary key not null,
"slide_id",
"type",
"props",
"x",
"y"
);

CREATE INDEX IF NOT EXISTS "shape_component_slide_id" ON "shape_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "line_component" ("id" primary key not null, "slide_id", "props");

CREATE INDEX IF NOT EXISTS "line_component_slide_id" ON "line_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "line_point" ("id" primary key not null, "line_id", "x", "y");

CREATE INDEX IF NOT EXISTS "line_point_line_id" ON "line_point" ("line_id");

CREATE INDEX IF NOT EXISTS "text_component_slide_id" ON "text_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "theme" (
"id" INTEGER primary key not null,
"name",
"bg_colorset",
"fg_colorset",
"fontset",
"surface_color",
"font_color"
);

CREATE TABLE IF NOT EXISTS "recent_color" (
"color" INTEGER primary key not null,
"last_used",
"first_used",
"theme_id"
);

CREATE TABLE IF NOT EXISTS "presenter" (
"name" primary key not null,
"available_transitions",
"picked_transition"
);

SELECT crsql_as_crr('deck');

SELECT crsql_as_crr('slide');

SELECT crsql_fract_as_ordered('slide', 'order', 'deck_id');

SELECT crsql_as_crr('text_component');

SELECT crsql_as_crr('embed_component');

SELECT crsql_as_crr('shape_component');

SELECT crsql_as_crr('line_component');

SELECT crsql_as_crr('line_point');

SELECT crsql_as_crr('theme');

SELECT crsql_as_crr('recent_color');

SELECT crsql_as_crr('presenter');

CREATE TABLE IF NOT EXISTS "selected_slide" (
"deck_id" not null,
"slide_id" not null,
primary key ("deck_id", "slide_id")
);

CREATE TABLE IF NOT EXISTS "selected_component" (
"slide_id" not null,
"component_id" not null,
"component_type",
primary key ("slide_id", "component_id")
);

CREATE TABLE IF NOT EXISTS "undo_stack" (
"deck_id" not null,
"operation",
"order" not null,
primary key ("deck_id", "order")
);

CREATE TABLE IF NOT EXISTS "redo_stack" (
"deck_id" not null,
"operation",
"order" not null,
primary key ("deck_id", "order")
);"#,
        sqlite::Destructor::STATIC,
    )
    .expect("migrate statement bound");
    stmt.step().expect("migrate statement stepped");
    assert_eq!(
        stmt.column_text(0).expect("got result"),
        "migration complete"
    );
    stmt.reset().expect("reset stmt");
    stmt.step().expect("migrate again");
    assert_eq!(
        stmt.column_text(0).expect("got result"),
        "migration complete"
    );

    // Now lets make change
    let stmt = db
        .db
        .prepare_v2(
            r#"
SELECT crsql_automigrate(?, 'SELECT crsql_finalize();')"#,
        )
        .expect("migrate statement prepared again?");
    stmt.bind_text(
      1,
      r#"
CREATE TABLE IF NOT EXISTS "deck" (
"id" INTEGER primary key not null,
"title",
"created",
"modified",
"theme_id",
"chosen_presenter"
);

CREATE TABLE IF NOT EXISTS "slide" (
"id" INTEGER primary key not null,
"deck_id",
"order",
"created",
"modified",
"x",
"y",
"z"
);

CREATE INDEX IF NOT EXISTS "slide_deck_id" ON "slide" ("deck_id", "order");

CREATE TABLE IF NOT EXISTS "text_component" (
"id" INTEGER primary key not null,
"slide_id",
"text",
"styles",
"x",
"y",
"width",
"height"
);

CREATE TABLE IF NOT EXISTS "embed_component" ("id" primary key not null, "slide_id", "src", "x", "y", "width", "height");

CREATE INDEX IF NOT EXISTS "embed_component_slide_id" ON "embed_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "shape_component" (
"id" INTEGER primary key not null,
"slide_id",
"type",
"props",
"x",
"y",
"width",
"height"
);

CREATE INDEX IF NOT EXISTS "shape_component_slide_id" ON "shape_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "line_component" ("id" primary key not null, "slide_id", "props");

CREATE INDEX IF NOT EXISTS "line_component_slide_id" ON "line_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "line_point" ("id" primary key not null, "line_id", "x", "y");

CREATE INDEX IF NOT EXISTS "line_point_line_id" ON "line_point" ("line_id");

CREATE INDEX IF NOT EXISTS "text_component_slide_id" ON "text_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "theme" (
"id" INTEGER primary key not null,
"name",
"bg_colorset",
"fg_colorset",
"fontset",
"surface_color",
"font_color"
);

CREATE TABLE IF NOT EXISTS "recent_color" (
"color" INTEGER primary key not null,
"last_used",
"first_used",
"theme_id"
);

CREATE TABLE IF NOT EXISTS "presenter" (
"name" primary key not null,
"available_transitions",
"picked_transition"
);

SELECT crsql_as_crr('deck');

SELECT crsql_as_crr('slide');

SELECT crsql_fract_as_ordered('slide', 'order', 'deck_id');

SELECT crsql_as_crr('text_component');

SELECT crsql_as_crr('embed_component');

SELECT crsql_as_crr('shape_component');

SELECT crsql_as_crr('line_component');

SELECT crsql_as_crr('line_point');

SELECT crsql_as_crr('theme');

SELECT crsql_as_crr('recent_color');

SELECT crsql_as_crr('presenter');

CREATE TABLE IF NOT EXISTS "selected_slide" (
"deck_id" not null,
"slide_id" not null,
primary key ("deck_id", "slide_id")
);

CREATE TABLE IF NOT EXISTS "selected_component" (
"slide_id" not null,
"component_id" not null,
"component_type",
primary key ("slide_id", "component_id")
);

CREATE TABLE IF NOT EXISTS "undo_stack" (
"deck_id" not null,
"operation",
"order" not null,
primary key ("deck_id", "order")
);

CREATE TABLE IF NOT EXISTS "redo_stack" (
"deck_id" not null,
"operation",
"order" not null,
primary key ("deck_id", "order")
);"#,
      sqlite::Destructor::STATIC,
  ).expect("bound");
    stmt.step().expect("stepped");
    assert_eq!(
        stmt.column_text(0).expect("completed"),
        "migration complete"
    );
}

fn empty_schema() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    let stmt = db
        .db
        .prepare_v2("SELECT crsql_automigrate('', 'SELECT crsql_finalize();')")?;
    stmt.step()?;
    assert_eq!(stmt.column_text(0)?, "migration complete");
    Ok(())
}

fn to_empty_from_something() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db.exec_safe("CREATE TABLE foo (a primary key, b);")?;
    db.db.exec_safe("CREATE TABLE bar (a, b, c);")?;
    db.db
        .exec_safe("CREATE TABLE item (id1 not null, id2 not null, x, primary key (id1, id2));")?;
    db.db.exec_safe("SELECT crsql_as_crr('item')")?;
    db.db
        .exec_safe("SELECT crsql_automigrate('', 'SELECT crsql_finalize();')")?;

    assert!(expect_tables(&db.db, vec![])?);

    Ok(())
}

fn to_something_from_empty() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    let schema = "
        CREATE TABLE IF NOT EXISTS foo (a primary key, b);
        CREATE TABLE IF NOT EXISTS bar (a not null, b not null, c, primary key(a, b));
        SELECT crsql_as_crr('bar');
        CREATE INDEX IF NOT EXISTS foo_b ON foo (b);
    ";
    invoke_automigrate(&db.db, schema)?;

    assert!(expect_tables(&db.db, vec!["foo", "bar"])?);
    assert!(expect_indices(
        &db.db,
        "foo",
        vec!["foo_b", "sqlite_autoindex_foo_1"]
    )?);

    Ok(())
}

fn add_col() -> Result<(), ResultCode> {
    // start with some table
    // move to a schema that adds a column to it
    let db = crate::opendb()?;
    db.db
        .exec_safe("CREATE TABLE todo (id primary key, content text)")?;
    let schema = "
        CREATE TABLE IF NOT EXISTS todo (
            id primary key,
            content text,
            complete integer
        );
    ";
    invoke_automigrate(&db.db, schema)?;

    assert!(expect_columns(
        &db.db,
        "todo",
        vec!["id", "content", "complete"],
    )?);

    Ok(())
}

fn remove_col() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db
        .exec_safe("CREATE TABLE todo (id primary key, content text, complete integer, list)")?;

    let schema = "
        CREATE TABLE IF NOT EXISTS todo (
            id primary key,
            content text,
            complete integer
        );
    ";
    invoke_automigrate(&db.db, schema)?;

    assert!(expect_columns(
        &db.db,
        "todo",
        vec!["id", "content", "complete"]
    )?);

    // test against a CRR?
    // technically you've unit tested crr migrations on their own
    // so.. automigrate should work fine with them.
    // famous last words.

    Ok(())
}

fn remove_col_fract_table() {
    let db = crate::opendb().expect("db opened");
    db.db
        .exec_safe("CREATE TABLE todo (id primary key not null, content text, position, thing)")
        .expect("table made");
    db.db
        .exec_safe("SELECT crsql_fract_as_ordered('todo', 'position');")
        .expect("as ordered");

    let schema = "
      CREATE TABLE IF NOT EXISTS todo (
          id primary key,
          content text,
          position
      );
  ";
    invoke_automigrate(&db.db, schema).expect("migrated");

    assert!(expect_columns(&db.db, "todo", vec!["id", "content", "position"]).expect("matched"));
}

fn remove_index() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db.exec_safe(
        "
        CREATE TABLE foo (a primary key, b);
        CREATE INDEX foo_b ON foo (b);
    ",
    )?;
    let schema = "CREATE TABLE IF NOT EXISTS foo (a primary key, b);";
    invoke_automigrate(&db.db, schema)?;

    assert!(expect_indices(
        &db.db,
        "foo",
        vec!["sqlite_autoindex_foo_1"]
    )?);

    Ok(())
}

fn add_index() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db.exec_safe("CREATE TABLE foo(a primary key, b);")?;
    let schema = "
        CREATE TABLE IF NOT EXISTS foo(a primary key, b);
        CREATE INDEX IF NOT EXISTS foo_b ON foo (b);
    ";
    invoke_automigrate(&db.db, schema)?;

    assert!(expect_indices(
        &db.db,
        "foo",
        vec!["sqlite_autoindex_foo_1", "foo_b"]
    )?);
    Ok(())
}

fn change_index_to_unique() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db.exec_safe(
        "
        CREATE TABLE foo (a primary key, b);
        CREATE INDEX foo_b ON foo (b);",
    )?;
    let schema = "
        CREATE TABLE IF NOT EXISTS foo(a primary key, b);
        CREATE UNIQUE INDEX IF NOT EXISTS foo_b ON foo (b);
    ";
    invoke_automigrate(&db.db, schema)?;

    // TODO: test index uniqueness
    assert!(expect_indices(
        &db.db,
        "foo",
        vec!["sqlite_autoindex_foo_1", "foo_b"]
    )?);
    Ok(())
}

fn remove_col_from_index() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db.exec_safe(
        "
        CREATE TABLE foo (a primary key, b, c);
        CREATE INDEX foo_boo ON foo (b, c);
    ",
    )?;
    let schema = "
        CREATE TABLE IF NOT EXISTS foo(a primary key, b, c);
        CREATE INDEX IF NOT EXISTS foo_boo ON foo (b);
    ";
    invoke_automigrate(&db.db, schema)?;

    // TODO: test index composition
    assert!(expect_indices(
        &db.db,
        "foo",
        vec!["sqlite_autoindex_foo_1", "foo_boo"]
    )?);
    Ok(())
}

fn add_col_to_index() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db.exec_safe(
        "
        CREATE TABLE foo (a primary key, b, c);
        CREATE INDEX foo_boo ON foo (b);
    ",
    )?;
    let schema = "
        CREATE TABLE IF NOT EXISTS foo(a primary key, b, c);
        CREATE INDEX IF NOT EXISTS foo_boo ON foo (b, c);
    ";
    invoke_automigrate(&db.db, schema)?;

    // TODO: test index composition
    assert!(expect_indices(
        &db.db,
        "foo",
        vec!["sqlite_autoindex_foo_1", "foo_boo"]
    )?);
    Ok(())
}

fn rename_col() -> Result<(), ResultCode> {
    let db = crate::opendb()?;
    db.db.exec_safe("CREATE TABLE foo (a primary key, b);")?;
    let schema = "
        CREATE TABLE IF NOT EXISTS foo (
            a primary key,
            c
        )
    ";
    invoke_automigrate(&db.db, schema)?;

    assert!(expect_columns(&db.db, "foo", vec!["a", "c"])?);
    Ok(())
}

fn expect_columns(
    db: &ManagedConnection,
    table: &str,
    expected: Vec<&str>,
) -> Result<bool, ResultCode> {
    let stmt = db.prepare_v2("SELECT name FROM pragma_table_info(?)")?;
    stmt.bind_text(1, table, sqlite::Destructor::STATIC)?;
    let mut len = 0;
    while stmt.step()? == ResultCode::ROW {
        let col = stmt.column_text(0)?;
        if !expected.contains(&col) {
            return Ok(false);
        }
        len += 1;
    }
    Ok(len == expected.len())
}

fn invoke_automigrate(db: &ManagedConnection, schema: &str) -> Result<ResultCode, ResultCode> {
    let stmt = db.prepare_v2("SELECT crsql_automigrate(?, 'SELECT crsql_finalize();');")?;
    stmt.bind_text(1, schema, sqlite::Destructor::STATIC)?;
    stmt.step()
}

fn expect_tables(db: &ManagedConnection, expected: Vec<&str>) -> Result<bool, ResultCode> {
    let stmt = db.prepare_v2(
        "SELECT name FROM pragma_table_list WHERE name NOT LIKE 'sqlite_%' AND name NOT LIKE '%crsql_%'"
    )?;

    let mut len = 0;
    while stmt.step()? == ResultCode::ROW {
        let tbl = stmt.column_text(0)?;
        if !expected.contains(&tbl) {
            return Ok(false);
        }
        len = len + 1;
    }

    Ok(len == expected.len())
}

fn expect_indices(
    db: &ManagedConnection,
    table: &str,
    expected: Vec<&str>,
) -> Result<bool, ResultCode> {
    let stmt = db.prepare_v2("SELECT name FROM pragma_index_list(?)")?;
    stmt.bind_text(1, table, sqlite::Destructor::STATIC)?;
    let mut len = 0;
    while stmt.step()? == ResultCode::ROW {
        let idx = stmt.column_text(0)?;
        if !expected.contains(&idx) {
            return Ok(false);
        }
        len = len + 1;
    }

    Ok(len == expected.len())
}

pub fn run_suite() -> Result<(), ResultCode> {
    empty_schema()?;
    to_empty_from_something()?;
    to_something_from_empty()?;
    add_col()?;
    remove_col()?;
    rename_col()?;
    remove_index()?;
    add_index()?;
    change_index_to_unique()?;
    remove_col_from_index()?;
    add_col_to_index()?;
    idempotent();
    change_index_col_order();
    add_many_cols();
    remove_many_cols();
    remove_indexed_cols();
    add_crr();
    add_table();
    remove_table();
    remove_crr();
    primary_key_change();
    with_default_value();
    not_null();
    nullable();
    no_default_value();
    strut_schema();
    Ok(())
}
