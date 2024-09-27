use clap::{Parser, Subcommand};
use sqlx_libsql::LibsqlPool;
use std::env;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Add { description: String },
    Done { id: i64 },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let pool = LibsqlPool::connect(&env::var("DATABASE_URL")?).await?;

    match args.cmd {
        Some(Command::Add { description }) => {
            println!("Adding new todo with description '{description}'");
            let todo_id = add_todo(&pool, description).await?;
            println!("Added new todo with id {todo_id}");
        }
        Some(Command::Done { id }) => {
            println!("Marking todo {id} as done");
            if complete_todo(&pool, id).await? {
                println!("Todo {id} is marked as done");
            } else {
                println!("Invalid id {id}");
            }
        }
        None => {
            println!("Printing list of all todos");
            list_todos(&pool).await?;
        }
    }

    Ok(())
}

async fn add_todo(pool: &LibsqlPool, description: String) -> anyhow::Result<i64> {
    let mut conn = pool.acquire().await?;

    // Insert the task, then obtain the ID of this row
    let id = sqlx::query(
        r#"
INSERT INTO todos ( description )
VALUES ( ?1 )
        "#,
    )
    .bind(description)
    .execute(&mut *conn)
    .await?
    .last_insert_rowid();

    Ok(id)
}

async fn complete_todo(pool: &LibsqlPool, id: i64) -> anyhow::Result<bool> {
    let rows_affected = sqlx::query(
        r#"
UPDATE todos
SET done = TRUE
WHERE id = ?1
        "#,
    )
    .bind(id)
    .execute(pool)
    .await?
    .rows_affected();

    Ok(rows_affected > 0)
}

async fn list_todos(pool: &LibsqlPool) -> anyhow::Result<()> {
    let recs = sqlx::query(
        r#"
SELECT id, description, done
FROM todos
ORDER BY id
        "#,
    )
    .fetch_all(pool)
    .await?;

    for _rec in recs {
        // TODO(lucio): impl this
        // println!(
        //     "- [{}] {}: {}",
        //     if rec.done { "x" } else { " " },
        //     rec.id,
        //     &rec.description,
        // );
    }

    Ok(())
}
