require "pg"

describe "PostgreSQL client" do
  it "connects" do
    conn = PG.connect(host: "127.0.0.1", port: 5000)
  end

  it "performs schema changes" do
    conn = PG.connect(host: "127.0.0.1", port: 5000)
    conn.exec("CREATE TABLE IF NOT EXISTS users (username TEXT)")
  end

  it "queries tables" do
    conn = PG.connect(host: "127.0.0.1", port: 5000)
    conn.exec("CREATE TABLE IF NOT EXISTS users (username TEXT)")
    conn.exec("SELECT * FROM users") do |results|
      puts results
    end
  end
end