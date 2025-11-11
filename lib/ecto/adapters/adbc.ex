defmodule Ecto.Adapters.Adbc do
  @moduledoc """
  Ecto adapter for DuckDB using ADBC.

  ## Options

    * `:database` - the path to the database or `:memory:` for in-memory database
    * `:driver` - ADBC driver to use (default: `:duckdb`)
    * `:version` - DuckDB version (default: `"1.4.0"`)
    * `:pool_size` - connection pool size (default: 5, must be 1 for `:memory:`)

  ## Type Configuration

    * `:binary_id_type` - how to store binary IDs (`:string` or `:binary`, default: `:string`)
    * `:uuid_type` - how to store UUIDs (`:string` or `:binary`, default: `:string`)
    * `:map_type` - how to store maps (`:string` or `:binary`, default: `:string`)
    * `:array_type` - how to store arrays (`:string` or `:binary`, default: `:string`)
    * `:json_library` - JSON library to use (default: `Jason`)

  ## Example

      defmodule MyApp.Repo do
        use Ecto.Repo,
          otp_app: :my_app,
          adapter: Ecto.Adapters.Adbc
      end

      config :my_app, MyApp.Repo,
        database: "path/to/db.duckdb",
        pool_size: 5
  """

  use Ecto.Adapters.SQL, driver: :ecto_adbc

  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  alias Ecto.Adapters.Adbc.{Codec, Connection}

  ## Adapter Callbacks

  @impl true
  def supports_ddl_transaction?, do: true

  @impl true
  def lock_for_migrations(_meta, _opts, fun) do
    # DuckDB doesn't support advisory locks, so we just run the function
    # This means migrations cannot be run concurrently
    fun.()
  end

  # Creates DuckLake metadata schemas during storage_up
  # This is called when the database is first created (mix ecto.create)
  defp create_ducklake_metadata_databases(conn, options) do
    attach_configs = options[:attach] || []

    Enum.each(attach_configs, fn attach_config ->
      {path, opts} = case attach_config do
        {p, o} when is_list(o) -> {p, o}
        {p, o, _conn_opts} -> {p, o}
      end

      as_name = opts[:as]

      if as_name && String.starts_with?(to_string(path), "ducklake:") do
        metadata_schema = "__ducklake_metadata_#{as_name}"
        create_schema_sql = "CREATE SCHEMA IF NOT EXISTS #{metadata_schema}"

        # INSTALL ducklake;
        # ATTACH 'ducklake:sample_phoenix_ducklake_setup.duckdb' AS my_ducklake;

        Duckex.query!(conn, create_schema_sql)
      end
    end)
  end

  ## Storage Callbacks

  @impl true
  def storage_up(opts) do
    database = Keyword.fetch!(opts, :database)
    pool_size = Keyword.get(options, :pool_size, 1)

    pool_size != 1 ->
      raise ArgumentError, """
        DuckDB databases must use a pool_size of 1
      """

    if database == ":memory:" do
      {:error, :already_up}
    else
      case File.exists?(database) do
        true ->
          {:error, :already_up}

        false ->
          # Create the database by connecting to it with ADBC directly
          # This will create a proper DuckDB database file
          driver = Keyword.get(opts, :driver, :duckdb)
          version = Keyword.get(opts, :version, "1.4.1")

          db_opts = [driver: driver, version: version, path: database]

          case Adbc.Database.start_link(db_opts) do
            {:ok, db} ->
              case Adbc.Connection.start_link(database: db) do
                {:ok, conn} ->
                  # Create DuckLake metadata databases if this repo uses DuckLake
                  create_ducklake_metadata_databases(conn, options)

                  GenServer.stop(conn)
                  GenServer.stop(db)
                  :ok

                {:error, reason} ->
                  GenServer.stop(db)
                  {:error, reason}
              end

            {:error, reason} ->
              {:error, reason}
          end
      end
    end
  end

  @impl true
  def storage_down(opts) do
    database = Keyword.fetch!(opts, :database)

    if database == ":memory:" do
      {:error, :already_down}
    else
      case File.exists?(database) do
        true -> File.rm(database)
        false -> {:error, :already_down}
      end
    end
  end

  @impl true
  def storage_status(opts) do
    database = Keyword.fetch!(opts, :database)

    if database == ":memory:" do
      :up
    else
      if File.exists?(database), do: :up, else: :down
    end
  end

  ## Structure Callbacks

  @impl true
  def structure_dump(default, config) do
    database = Keyword.fetch!(config, :database)

    case run_with_cmd(database, ".schema", []) do
      {output, 0} ->
        File.write!(default, output)
        {:ok, default}

      {output, _} ->
        {:error, output}
    end
  end

  @impl true
  def structure_load(default, config) do
    database = Keyword.fetch!(config, :database)

    case File.read(default) do
      {:ok, contents} ->
        case run_with_cmd(database, contents, []) do
          {_, 0} -> {:ok, default}
          {output, _} -> {:error, output}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def dump_cmd(args, opts \\ [], config) do
    database = Keyword.fetch!(config, :database)

    case database do
      ":memory:" ->
        {:error, "Cannot dump :memory: database"}

      path ->
        # Use duckdb CLI to dump schema
        # The -c flag runs a command and exits
        args = [path, "-c", ".schema"] ++ args
        {:ok, "duckdb", args, opts}
    end
  end

  ## Type Handling

  @impl true
  def loaders(:boolean, type), do: [&Codec.bool_decode/1, type]
  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(:utc_datetime, type), do: [&Codec.utc_datetime_decode/1, type]
  def loaders(:utc_datetime_usec, type), do: [&Codec.utc_datetime_decode/1, type]
  def loaders(:naive_datetime, type), do: [&Codec.naive_datetime_decode/1, type]
  def loaders(:naive_datetime_usec, type), do: [&Codec.naive_datetime_decode/1, type]
  def loaders(:date, type), do: [&Codec.date_decode/1, type]
  def loaders(:time, type), do: [&Codec.time_decode/1, type]
  def loaders(:time_usec, type), do: [&Codec.time_decode/1, type]
  def loaders(:map, type), do: [&Codec.json_decode/1, type]
  def loaders({:map, _}, type), do: [&Codec.json_decode/1, type]
  def loaders({:array, _}, type), do: [&Codec.json_decode/1, type]
  def loaders(:decimal, type), do: [&Codec.decimal_decode/1, type]
  def loaders(:float, type), do: [&Codec.float_decode/1, type]
  def loaders(_primitive, type), do: [type]

  @impl true
  def dumpers(:binary, type), do: [type, &Codec.blob_encode/1]
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(:boolean, type), do: [type, &Codec.bool_encode/1]
  def dumpers(:map, type), do: [&Codec.json_encode/1, type]
  def dumpers({:map, _}, type), do: [&Codec.json_encode/1, type]
  def dumpers({:array, _}, type), do: [&Codec.json_encode/1, type]
  def dumpers(:utc_datetime, type), do: [type, &Codec.utc_datetime_encode/1]
  def dumpers(:utc_datetime_usec, type), do: [type, &Codec.utc_datetime_encode/1]
  def dumpers(:naive_datetime, type), do: [type, &Codec.naive_datetime_encode/1]
  def dumpers(:naive_datetime_usec, type), do: [type, &Codec.naive_datetime_encode/1]
  def dumpers(:date, type), do: [type, &Codec.date_encode/1]
  def dumpers(:time, type), do: [type, &Codec.time_encode/1]
  def dumpers(:time_usec, type), do: [type, &Codec.time_encode/1]
  def dumpers(:decimal, type), do: [type, &Codec.decimal_encode/1]
  def dumpers(_primitive, type), do: [type]

  ## Autogenerate

  @impl true
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  ## Private Helpers

  defp run_with_cmd(database, sql, _args) do
    # For now, we'll use a simple approach - in production you might want
    # to use the DuckDB CLI or another method
    # This is a simplified version
    case Connection.start_link(database: database) do
      {:ok, conn} ->
        case Connection.query(conn, sql, [], []) do
          {:ok, result} ->
            GenServer.stop(conn)
            {inspect(result), 0}

          {:error, error} ->
            GenServer.stop(conn)
            {inspect(error), 1}
        end

      {:error, error} ->
        {inspect(error), 1}
    end
  end
end
