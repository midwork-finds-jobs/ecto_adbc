You need to create a ecto wrapper for duckdb using duckdb through adbc.

First build DB Connection behaviour for adbc.

# Elixir + DuckDB + Iceberg

```elixir
Mix.install([
  {:explorer, "~> 0.11.1"},
  {:adbc, github: "elixir-explorer/adbc", ref: "8e44fc402627dd0c4c6077c24df27cf8a97654cd", override: true},
  {:kino, "~> 0.17.0"},
  {:kino_explorer, "~> 0.1.25"},
  {:kino_vega_lite, "~> 0.1.13"},
])
```

## Connect to DuckDB

```elixir
# Change URL for your architecture: https://github.com/duckdb/duckdb/releases/tag/v1.4.0
Adbc.download_driver!(:duckdb, version: "1.4.0", url: "https://github.com/duckdb/duckdb/releases/download/v1.4.0/libduckdb-linux-arm64.zip", force: true)

{:ok, db} = Kino.start_child({Adbc.Database, driver: :duckdb, version: "1.4.0"})
{:ok, conn} = Kino.start_child({Adbc.Connection, database: db})
```

## Load Extensions

```elixir
Adbc.Connection.query!(conn, "INSTALL iceberg;")
Adbc.Connection.query!(conn, "INSTALL httpfs;")
Adbc.Connection.query!(conn, "UPDATE extensions;")
Adbc.Connection.query!(conn, "LOAD iceberg;")
Adbc.Connection.query!(conn, "LOAD httpfs;")
```

Your goal is to provide ecto functionality for duckdb which is used through the adbc library: https://github.com/livebook-dev/adbc

You can have a look on other ecto libraries for duckdb in: `../ecto_duckdb` and `../ecto_duckdb_test`

When you're ready create a sample phoenix project. Configurate that with the duckdb as main database repo. Create a migration for that repo which creates table countries with country name and capital name.