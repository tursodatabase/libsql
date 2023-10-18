use sqlite::{Connection, ManagedConnection, ResultCode};
use sqlite_nostd as sqlite;

// TODO: auto-calculate starting number
integration_utils::counter_setup!(26);

#[test]
fn empty_schema() {
    empty_schema_impl().unwrap();
    decrement_counter();
}

#[test]
fn to_empty_from_something() {
    to_empty_from_something_impl().unwrap();
    decrement_counter();
}

#[test]
fn to_something_from_empty() {
    to_something_from_empty_impl().unwrap();
    decrement_counter();
}

#[test]
fn idempotent() {
    from_something_to_same_impl().unwrap();
    decrement_counter();
}

#[test]
fn add_col() {
    add_col_impl().unwrap();
    decrement_counter();
}

#[test]
fn remove_col() {
    remove_col_impl().unwrap();
    decrement_counter();
}

#[test]
fn rename_col() {
    rename_col_impl().unwrap();
    decrement_counter();
}

#[test]
fn remove_index() {
    remove_index_impl().unwrap();
    decrement_counter();
}

#[test]
fn add_index() {
    add_index_impl().unwrap();
    decrement_counter();
}

#[test]
fn change_index_to_unique() {
    change_index_to_unique_impl().unwrap();
    decrement_counter();
}

#[test]
fn remove_col_from_index() {
    remove_col_from_index_impl().unwrap();
    decrement_counter();
}

#[test]
fn add_col_to_index() {
    add_col_to_index_impl().unwrap();
    decrement_counter();
}

#[test]
fn change_index_col_order() {
    decrement_counter();
}

#[test]
fn add_many_cols() {
    decrement_counter();
}

#[test]
fn remove_many_cols() {
    decrement_counter();
}

#[test]
fn remove_indexed_cols() {
    decrement_counter();
}

#[test]
fn add_crr() {
    decrement_counter();
}

#[test]
fn add_table() {
    decrement_counter();
}

#[test]
fn remove_table() {
    decrement_counter();
}

#[test]
fn remove_crr() {
    decrement_counter();
}

#[test]
fn primary_key_change() {
    decrement_counter();
}

#[test]
fn with_default_value() {
    decrement_counter();
}

#[test]
fn not_null() {
    decrement_counter();
}

#[test]
fn nullable() {
    decrement_counter();
}

#[test]
fn no_default_value() {
    decrement_counter();
}

#[test]
fn strut_schema() {
    strut_schema_impl().unwrap();
    decrement_counter();
}

fn strut_schema_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let stmt = db.db.prepare_v2(
        r#"
SELECT crsql_automigrate(?)"#,
    )?;
    stmt.bind_text(
        1,
        r#"
CREATE TABLE IF NOT EXISTS "deck" (
  "id" INTEGER primary key,
  "title",
  "created",
  "modified",
  "theme_id",
  "chosen_presenter"
);

CREATE TABLE IF NOT EXISTS "slide" (
  "id" INTEGER primary key,
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
  "id" INTEGER primary key,
  "slide_id",
  "text",
  "styles",
  "x",
  "y"
);

CREATE TABLE IF NOT EXISTS "embed_component" ("id" primary key, "slide_id", "src", "x", "y");

CREATE INDEX IF NOT EXISTS "embed_component_slide_id" ON "embed_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "shape_component" (
  "id" INTEGER primary key,
  "slide_id",
  "type",
  "props",
  "x",
  "y"
);

CREATE INDEX IF NOT EXISTS "shape_component_slide_id" ON "shape_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "line_component" ("id" primary key, "slide_id", "props");

CREATE INDEX IF NOT EXISTS "line_component_slide_id" ON "line_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "line_point" ("id" primary key, "line_id", "x", "y");

CREATE INDEX IF NOT EXISTS "line_point_line_id" ON "line_point" ("line_id");

CREATE INDEX IF NOT EXISTS "text_component_slide_id" ON "text_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "theme" (
  "id" INTEGER primary key,
  "name",
  "bg_colorset",
  "fg_colorset",
  "fontset",
  "surface_color",
  "font_color"
);

CREATE TABLE IF NOT EXISTS "recent_color" (
  "color" INTEGER primary key,
  "last_used",
  "first_used",
  "theme_id"
);

CREATE TABLE IF NOT EXISTS "presenter" (
  "name" primary key,
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
  "deck_id",
  "slide_id",
  primary key ("deck_id", "slide_id")
);

CREATE TABLE IF NOT EXISTS "selected_component" (
  "slide_id",
  "component_id",
  "component_type",
  primary key ("slide_id", "component_id")
);

CREATE TABLE IF NOT EXISTS "undo_stack" (
  "deck_id",
  "operation",
  "order",
  primary key ("deck_id", "order")
);

CREATE TABLE IF NOT EXISTS "redo_stack" (
  "deck_id",
  "operation",
  "order",
  primary key ("deck_id", "order")
);"#,
        sqlite::Destructor::STATIC,
    )?;
    stmt.step()?;
    assert_eq!(stmt.column_text(0)?, "migration complete");
    stmt.reset()?;
    stmt.step()?;
    assert_eq!(stmt.column_text(0)?, "migration complete");

    // Now lets make change
    let stmt = db.db.prepare_v2(
        r#"
SELECT crsql_automigrate(?)"#,
    )?;
    stmt.bind_text(
        1,
        r#"
CREATE TABLE IF NOT EXISTS "deck" (
"id" INTEGER primary key,
"title",
"created",
"modified",
"theme_id",
"chosen_presenter"
);

CREATE TABLE IF NOT EXISTS "slide" (
"id" INTEGER primary key,
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
"id" INTEGER primary key,
"slide_id",
"text",
"styles",
"x",
"y",
"width",
"height"
);

CREATE TABLE IF NOT EXISTS "embed_component" ("id" primary key, "slide_id", "src", "x", "y", "width", "height");

CREATE INDEX IF NOT EXISTS "embed_component_slide_id" ON "embed_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "shape_component" (
"id" INTEGER primary key,
"slide_id",
"type",
"props",
"x",
"y",
"width",
"height"
);

CREATE INDEX IF NOT EXISTS "shape_component_slide_id" ON "shape_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "line_component" ("id" primary key, "slide_id", "props");

CREATE INDEX IF NOT EXISTS "line_component_slide_id" ON "line_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "line_point" ("id" primary key, "line_id", "x", "y");

CREATE INDEX IF NOT EXISTS "line_point_line_id" ON "line_point" ("line_id");

CREATE INDEX IF NOT EXISTS "text_component_slide_id" ON "text_component" ("slide_id");

CREATE TABLE IF NOT EXISTS "theme" (
"id" INTEGER primary key,
"name",
"bg_colorset",
"fg_colorset",
"fontset",
"surface_color",
"font_color"
);

CREATE TABLE IF NOT EXISTS "recent_color" (
"color" INTEGER primary key,
"last_used",
"first_used",
"theme_id"
);

CREATE TABLE IF NOT EXISTS "presenter" (
"name" primary key,
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
"deck_id",
"slide_id",
primary key ("deck_id", "slide_id")
);

CREATE TABLE IF NOT EXISTS "selected_component" (
"slide_id",
"component_id",
"component_type",
primary key ("slide_id", "component_id")
);

CREATE TABLE IF NOT EXISTS "undo_stack" (
"deck_id",
"operation",
"order",
primary key ("deck_id", "order")
);

CREATE TABLE IF NOT EXISTS "redo_stack" (
"deck_id",
"operation",
"order",
primary key ("deck_id", "order")
);"#,
        sqlite::Destructor::STATIC,
    )?;
    stmt.step()?;
    assert_eq!(stmt.column_text(0)?, "migration complete");

    Ok(())
}

fn empty_schema_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let stmt = db.db.prepare_v2("SELECT crsql_automigrate('')")?;
    stmt.step()?;
    assert_eq!(stmt.column_text(0)?, "migration complete");
    Ok(())
}

fn to_empty_from_something_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    db.db.exec_safe("CREATE TABLE foo (a primary key, b);")?;
    db.db.exec_safe("CREATE TABLE bar (a, b, c);")?;
    db.db
        .exec_safe("CREATE TABLE item (id1, id2, x, primary key (id1, id2));")?;
    db.db.exec_safe("SELECT crsql_as_crr('item')")?;
    db.db.exec_safe("SELECT crsql_automigrate('')")?;

    assert!(expect_tables(&db.db, vec![])?);

    Ok(())
}

fn to_something_from_empty_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let schema = "
        CREATE TABLE IF NOT EXISTS foo (a primary key, b);
        CREATE TABLE IF NOT EXISTS bar (a, b, c, primary key(a, b));
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

fn from_something_to_same_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
    let schema = "
        CREATE TABLE IF NOT EXISTS item (id integer primary key, data any) strict;
        CREATE TABLE IF NOT EXISTS container (id integer primary key, contained integer);
        CREATE INDEX IF NOT EXISTS container_contained ON container (contained);
        SELECT crsql_as_crr('item');
    ";
    db.db.exec_safe(schema)?;
    invoke_automigrate(&db.db, schema)?;

    assert!(expect_tables(&db.db, vec!["item", "container"])?);
    assert!(expect_indices(
        &db.db,
        "container",
        vec!["container_contained"]
    )?);

    Ok(())
}

fn add_col_impl() -> Result<(), ResultCode> {
    // start with some table
    // move to a schema that adds a column to it
    let db = integration_utils::opendb()?;
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

fn remove_col_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
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

#[test]
fn remove_col_fract_table() {
    let db = integration_utils::opendb().expect("db opened");
    db.db
        .exec_safe("CREATE TABLE todo (id primary key, content text, position, thing)")
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

fn remove_index_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
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

fn add_index_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
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

fn change_index_to_unique_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
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

fn remove_col_from_index_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
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

fn add_col_to_index_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
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

fn rename_col_impl() -> Result<(), ResultCode> {
    let db = integration_utils::opendb()?;
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
    let stmt = db.prepare_v2("SELECT crsql_automigrate(?);")?;
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
