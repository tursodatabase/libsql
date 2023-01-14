 
const { Client } = require('pg')

test('Connect to server', async () => {
    const client = new Client({
        host: "127.0.0.1",
        port: 5432,
    })
    await client.connect()
    await client.end()
})

test('Change schema', async () => {
    const client = new Client({
        host: "127.0.0.1",
        port: 5432,
    })
    await client.connect()
    await client.query("CREATE TABLE IF NOT EXISTS users (username TEXT)")
    await client.end()
})

test('Query tables', async () => {
    const client = new Client({
        host: "127.0.0.1",
        port: 5432,
    })
    await client.connect()
    await client.query("CREATE TABLE IF NOT EXISTS users (username TEXT)")
    await client.query("SELECT * FROM users")
    await client.end()
})
