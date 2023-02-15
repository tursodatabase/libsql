import asyncio
import libsql_client
import random

url = "http://localhost:8080"

async def main():
    async with libsql_client.Client(url) as client:
        await client.batch([
            """
            CREATE TABLE IF NOT EXISTS book (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                author_id INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS author (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
            """,
        ])

        author_res = await client.execute(
            "INSERT INTO author (name) VALUES (?) RETURNING id",
            [sample_name(AUTHOR_NAME_PARTS)],
        )
        author_id = author_res.rows[0][0]

        book_count = random.randint(1, 3)
        for _ in range(book_count):
            await client.execute(
                "INSERT INTO book (title, author_id) VALUES (?, ?)",
                [sample_name(BOOK_TITLE_PARTS), author_id],
            )

        books_res = await client.execute(
            """
            SELECT b.id, b.title, a.id, a.name
            FROM book b JOIN author a ON b.author_id = a.id
            ORDER BY b.id ASC
            """
        )

        for row in books_res.rows:
            print(row)

AUTHOR_NAME_PARTS = [
    ["Daniel", "Jane", "Mark", "William", "Milan", "Kazuo", "Sally", "Mieko", "Kim"],
    ["Defoe", "Austen", "Twain", "Golding", "Kundera", "Ishiguro", "Rooney", "Kawakami", "Hye-Jin"],
]

BOOK_TITLE_PARTS = [
    [
        "Robinson", "Pride", "Sense", "Huckleberry", "Tom", "Lord",
        "Život", "Klara", "Normal", "Breasts", "Concerning",
    ],
    [
        "Crusoe", "and Prejudice", "and Sensibility", "Finn", "Sawyer", "of the Flies",
        "je jinde", "and The Sun", "People", "and Eggs", "My Daughter",
    ],
]

def sample_name(name_parts):
    return " ".join([
        random.choice(parts)
        for parts in name_parts
    ])

asyncio.run(main())
