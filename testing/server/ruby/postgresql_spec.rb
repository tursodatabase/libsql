require "pg"

db_uri = ENV["DB_URI"]

if db_uri.nil?
    raise "Please configure database via the `DB_URI` environment variable."
end

describe "PostgreSQL client" do
  it "connects" do
    conn = PG.connect(db_uri)
  end

  it "performs schema changes" do
    conn = PG.connect(db_uri)
    conn.exec("CREATE TABLE IF NOT EXISTS users (username TEXT, pass TEXT)")
  end

  it "queries tables" do
    conn = PG.connect(db_uri)
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
