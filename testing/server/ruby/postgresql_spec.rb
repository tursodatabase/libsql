require "pg"

describe "PostgreSQL client" do
  it "connects" do
    conn = PG.connect(host: "127.0.0.1", port: 5432)
  end

  it "performs schema changes" do
    conn = PG.connect(host: "127.0.0.1", port: 5432)
    conn.exec("CREATE TABLE IF NOT EXISTS users (username TEXT, pass TEXT)")
  end

  it "queries tables" do
    conn = PG.connect(host: "127.0.0.1", port: 5432)
    conn.exec("CREATE TABLE IF NOT EXISTS users (username TEXT, pass TEXT)")
    conn.exec("DELETE FROM users")
    conn.exec("INSERT INTO users VALUES ('me', 'my_pass')")
    conn.exec("SELECT * FROM users") do |results|
      results.each do |row|
          expect(row["username"]).to eq("me")
          expect(row["pass"]).to eq("my_pass")
      end
    end
  end
end
