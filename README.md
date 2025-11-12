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
  version: "1.4.1",
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

## DuckLake Support

DuckLake is an open table format that stores metadata in DuckDB and data in Parquet files, providing ACID transactions, schema evolution, and time travel capabilities.

### Configuration

```elixir
# config/config.exs
config :my_app, MyApp.DuckLakeRepo,
  database: "ducklake:path/to/database.ducklake",
  driver: :duckdb,
  version: "1.4.1",
  pool_size: 1,
  timeout: 600_000,  # 10 minutes for large operations
  # DuckLake doesn't support PRIMARY KEY constraints
  migration_primary_key: false,
  # DuckLake-specific options
  ducklake_options: [
    parquet_compression: :zstd,
    parquet_compression_level: 10,
    data_inlining_row_limit: 10000
  ]
```

### Repository Definition

```elixir
defmodule MyApp.DuckLakeRepo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Adbc

  @doc """
  Applies DuckLake options and loads extensions at runtime.
  """
  def init(_type, config) do
    config = Keyword.put(config, :after_connect, fn conn ->
      # Apply DuckLake options from adapter
      Ecto.Adapters.Adbc.ducklake_after_connect(conn, config)

      # Load custom extensions if needed
      load_extension(conn, "spatial")

      :ok
    end)

    {:ok, config}
  end

  defp load_extension(conn, extension_name) do
    query = %Adbcex.Query{statement: "LOAD #{extension_name}"}

    case DBConnection.execute(conn, query, [], []) do
      {:ok, _, _} -> :ok
      {:error, _} -> :ok  # Ignore if already loaded or not installed
    end
  end
end
```

### DuckLake Options

Configure DuckLake-specific options via `:ducklake_options`:

See more in https://ducklake.select/docs/stable/duckdb/usage/configuration

### Usage

```elixir
# Create the DuckLake database
mix ecto.create -r MyApp.DuckLakeRepo

# Run migrations
mix ecto.migrate -r MyApp.DuckLakeRepo --pool-size 1

# Use the repo
MyApp.DuckLakeRepo.insert!(%MyApp.AnalyticsEvent{...})
events = MyApp.DuckLakeRepo.all(MyApp.AnalyticsEvent)
```

### Benefits of DuckLake

- **Column-oriented storage**: Efficient analytics queries on large datasets
- **ACID transactions**: Full transactional guarantees
- **Time travel**: Query historical data at specific points in time
- **Schema evolution**: Add/modify columns without rewriting data
- **Open format**: Data stored in standard Parquet files for interoperability
- **Efficient compression**: Configurable compression for storage optimization

## Known Limitations

### Critical Limitations
- ❌ **No cursor support** - cannot stream large result sets
- ❌ **No advisory locks** - migrations cannot run concurrently
- ❌ **No row-level locks** - `FOR UPDATE` not supported
- ❌ **No PRIMARY KEY for ducklake** - Ducklake doesn't support PRIMARY KEY. Because of this if Ducklake is used in any database repo all other DuckDB repos will not get PRIMARY key for their `schema_migrations` table.
- ⚠️ **Requires `pool_size: 1`** for file-based databases

### Data Type Storage
- ⚠️ Dates/times stored as TEXT (ISO8601 format)
- ⚠️ JSON/maps stored as TEXT (JSON-encoded)
- ⚠️ UUIDs stored as TEXT

### See Full Documentation
For a complete list of limitations and workarounds, see [LIMITATIONS.md](LIMITATIONS.md).

## License

MIT License - See LICENSE file for details.
