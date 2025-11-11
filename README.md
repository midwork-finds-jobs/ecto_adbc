# Ecto ADBC Adapter

An Ecto adapter for DuckDB using Apache Arrow ADBC (Arrow Database Connectivity).

## Features

- ✅ Full CRUD operations (INSERT, SELECT, UPDATE, DELETE)
- ✅ Schema migrations and DDL operations
- ✅ Transactions (BEGIN, COMMIT, ROLLBACK)
- ✅ Auto-increment IDs using DuckDB sequences
- ✅ INSERT...RETURNING support
- ✅ Indexes and constraints
- ✅ Prepared statements with parameter binding
- ✅ Type encoding/decoding

## Installation

Add `ecto_adbc` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ecto_adbc, path: "../ecto_adbc"},
    {:ecto_sql, "~> 3.0"},
    {:jason, "~> 1.0"}  # JSON library
  ]
end
```

## Configuration

```elixir
# config/config.exs
config :my_app, MyApp.Repo,
  database: "path/to/database.duckdb",  # or ":memory:" for in-memory
  driver: :duckdb,
  version: "1.4.0",
  pool_size: 1  # IMPORTANT: Use 1 for file-based databases

# Define your repo
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Adbc
end
```

## Usage

```elixir
# Create the database
mix ecto.create

# Run migrations (note: use --pool-size 1)
mix ecto.migrate --pool-size 1

# Define a schema
defmodule MyApp.User do
  use Ecto.Schema

  schema "users" do
    field :name, :string
    field :email, :string
    timestamps()
  end
end

# Use the repo
MyApp.Repo.insert!(%MyApp.User{name: "Alice", email: "alice@example.com"})
users = MyApp.Repo.all(MyApp.User)
```

## Known Limitations

### Critical Limitations
- ❌ **No cursor support** - cannot stream large result sets
- ❌ **No advisory locks** - migrations cannot run concurrently
- ❌ **No row-level locks** - `FOR UPDATE` not supported
- ⚠️ **Requires `pool_size: 1`** for file-based databases

### Data Type Storage
- ⚠️ Dates/times stored as TEXT (ISO8601 format)
- ⚠️ JSON/maps stored as TEXT (JSON-encoded)
- ⚠️ UUIDs stored as TEXT

### See Full Documentation
For a complete list of limitations and workarounds, see [LIMITATIONS.md](LIMITATIONS.md).

## Example Phoenix Project

See the `duckdb_sample` directory for a complete Phoenix application example.

## License

MIT License - See LICENSE file for details.
