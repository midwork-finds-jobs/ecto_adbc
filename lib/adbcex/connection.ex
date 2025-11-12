defmodule Adbcex.Connection do
  @moduledoc """
  DBConnection adapter for ADBC (DuckDB).
  """

  use DBConnection

  alias Adbcex.{Error, Query, Result}

  defstruct [:db, :conn]

  @type state :: %__MODULE__{
          db: pid() | nil,
          conn: pid() | nil
        }

  @impl true
  def connect(opts) do
    database = Keyword.get(opts, :database, ":memory:")
    driver = Keyword.get(opts, :driver, :duckdb)
    version = Keyword.get(opts, :version, "1.4.0")

    # Ensure the driver is downloaded before attempting to use it
    ensure_driver_downloaded(driver, version)

    # Start ADBC Database
    db_opts = [driver: driver, version: version]
    db_opts = if database != ":memory:", do: Keyword.put(db_opts, :path, database), else: db_opts

    case Adbc.Database.start_link(db_opts) do
      {:ok, db} ->
        # Start ADBC Connection
        case Adbc.Connection.start_link(database: db) do
          {:ok, conn} ->
            {:ok, %__MODULE__{db: db, conn: conn}}

          {:error, reason} ->
            {:error, %Error{message: "Failed to create ADBC connection: #{inspect(reason)}"}}
        end

      {:error, reason} ->
        {:error, %Error{message: "Failed to open ADBC database: #{inspect(reason)}"}}
    end
  end

  @impl true
  def disconnect(_err, %__MODULE__{db: db, conn: conn}) do
    if conn && Process.alive?(conn), do: GenServer.stop(conn, :normal)
    if db && Process.alive?(db), do: GenServer.stop(db, :normal)
    :ok
  end

  @impl true
  def checkout(state), do: {:ok, state}

  @impl true
  def ping(state), do: {:ok, state}

  @impl true
  def handle_begin(_opts, state) do
    case exec_query("BEGIN", [], state) do
      {:ok, _result, state} -> {:ok, nil, state}
      {:error, err, state} -> {:disconnect, err, state}
    end
  end

  @impl true
  def handle_commit(_opts, state) do
    case exec_query("COMMIT", [], state) do
      {:ok, _result, state} -> {:ok, nil, state}
      {:error, err, state} -> {:disconnect, err, state}
    end
  end

  @impl true
  def handle_rollback(_opts, state) do
    case exec_query("ROLLBACK", [], state) do
      {:ok, _result, state} -> {:ok, nil, state}
      {:error, err, state} -> {:disconnect, err, state}
    end
  end

  @impl true
  def handle_status(_opts, state) do
    {:idle, state}
  end

  @impl true
  def handle_prepare(query, _opts, state) do
    {:ok, query, state}
  end

  @impl true
  def handle_execute(%Query{statement: statement} = query, params, _opts, state) do
    case exec_query(statement, params, state) do
      {:ok, result, state} -> {:ok, query, result, state}
      {:error, err, state} -> {:error, err, state}
    end
  end

  @impl true
  def handle_close(_query, _opts, state) do
    {:ok, nil, state}
  end

  @impl true
  def handle_declare(_query, _params, _opts, state) do
    {:error, %Error{message: "Cursors are not supported"}, state}
  end

  @impl true
  def handle_fetch(_query, _cursor, _opts, state) do
    {:error, %Error{message: "Cursors are not supported"}, state}
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state) do
    {:error, %Error{message: "Cursors are not supported"}, state}
  end

  # Private functions

  defp ensure_driver_downloaded(driver, version) do
    # Attempt to download the driver if it doesn't exist
    # This will automatically detect the correct URL for the current platform
    require Logger

    # Construct the download URL for the driver
    {os, arch} = get_platform()
    url = build_driver_url(driver, version, os, arch)

    try do
      Logger.info("Downloading #{driver} driver version #{version} for #{os}-#{arch}...")
      Adbc.download_driver!(driver, version: version, url: url)
      Logger.info("Driver #{driver} version #{version} downloaded successfully")
    rescue
      e in RuntimeError ->
        # If driver already exists, Adbc.download_driver! raises "already downloaded"
        if String.contains?(e.message, "already downloaded") do
          :ok
        else
          Logger.warning("Failed to download driver: #{inspect(e)}")
          reraise e, __STACKTRACE__
        end

      e ->
        require Logger
        Logger.error("Unexpected error downloading driver: #{inspect(e)}")
        reraise e, __STACKTRACE__
    end
  end

  defp get_platform() do
    # Detect OS and architecture
    os = case :os.type() do
      {:unix, :darwin} -> "osx"
      {:unix, :linux} -> "linux"
      {:win32, _} -> "windows"
      _ -> raise "Unsupported OS"
    end

    arch = case :erlang.system_info(:system_architecture) |> to_string() do
      "aarch64" <> _ -> "aarch64"
      "arm64" <> _ -> "aarch64"
      "x86_64" <> _ -> "amd64"
      "i686" <> _ -> "i686"
      _ -> raise "Unsupported architecture"
    end

    {os, arch}
  end

  defp build_driver_url(:duckdb, version, os, arch) do
    # Map OS names to DuckDB release naming
    os_name = case os do
      "osx" -> "osx"
      "linux" -> "linux"
      "windows" -> "windows"
    end

    # Map arch names to DuckDB release naming
    arch_name = case {os, arch} do
      {"osx", "aarch64"} -> "universal"
      {"osx", "amd64"} -> "universal"
      {"linux", "aarch64"} -> "aarch64"
      {"linux", "amd64"} -> "amd64"
      {"windows", "amd64"} -> "amd64"
      _ -> raise "Unsupported platform: #{os}-#{arch}"
    end

    "https://github.com/duckdb/duckdb/releases/download/v#{version}/libduckdb-#{os_name}-#{arch_name}.zip"
  end

  defp exec_query(statement, params, %__MODULE__{conn: conn} = state) do
    case Adbc.Connection.query(conn, statement, params, []) do
      {:ok, %Adbc.Result{data: data, num_rows: num_rows}} ->
        result = format_result(statement, data, num_rows)
        {:ok, result, state}

      {:error, %Adbc.Error{message: message}} ->
        {:error, %Error{message: message}, state}

      {:error, reason} ->
        {:error, %Error{message: inspect(reason)}, state}
    end
  end

  defp format_result(statement, data, num_rows) do
    command = parse_command(statement)

    case data do
      %Explorer.DataFrame{} = df ->
        # Convert DataFrame to columns and rows
        columns = Explorer.DataFrame.names(df)
        rows = df |> Explorer.DataFrame.to_rows() |> Enum.map(&Map.values/1)

        %Result{
          command: command,
          columns: columns,
          rows: rows,
          num_rows: num_rows || length(rows)
        }

      columns when is_list(columns) and length(columns) > 0 ->
        # Handle Adbc.Column format (used for some queries like COUNT, RETURNING)
        # Extract column names and convert data to rows
        column_names = Enum.map(columns, & &1.name)

        # Materialize Arrow references and convert column-oriented data to row-oriented
        materialized_columns =
          Enum.map(columns, fn column ->
            column
            |> Adbc.Column.materialize()
            |> Adbc.Column.to_list()
          end)

        # Transpose columns to rows
        rows =
          if Enum.any?(materialized_columns, &(&1 != [])) do
            materialized_columns
            |> Enum.zip()
            |> Enum.map(&Tuple.to_list/1)
          else
            []
          end

        # For INSERT/UPDATE/DELETE, DuckDB returns a "Count" column with affected rows
        # BUT if there's a RETURNING clause, it returns the actual data columns
        # Extract count only if it's the "Count" column
        actual_num_rows =
          if command in [:insert, :update, :delete] && length(columns) == 1 && hd(columns).name == "Count" do
            case hd(materialized_columns) do
              [count] when is_integer(count) -> count
              _ -> if num_rows == 0 || num_rows == nil, do: length(rows), else: num_rows
            end
          else
            # For RETURNING clauses, num_rows should be the number of rows returned
            if num_rows == 0 || num_rows == nil, do: length(rows), else: num_rows
          end

        %Result{
          command: command,
          columns: column_names,
          rows: rows,
          num_rows: actual_num_rows
        }

      [] ->
        # Empty result (typically from DDL statements or empty SELECT)
        # For SELECT queries, we need to return an empty list, not nil
        %Result{
          command: command,
          columns: [],
          rows: [],
          num_rows: num_rows || 0
        }

      nil ->
        # No result at all
        %Result{
          command: command,
          columns: [],
          rows: [],
          num_rows: num_rows || 0
        }
    end
  end

  defp parse_command(statement) do
    statement
    |> String.trim()
    |> String.upcase()
    |> String.split(" ", parts: 2)
    |> hd()
    |> case do
      "SELECT" -> :select
      "INSERT" -> :insert
      "UPDATE" -> :update
      "DELETE" -> :delete
      "CREATE" -> :create
      "DROP" -> :drop
      "ALTER" -> :alter
      "BEGIN" -> :begin
      "COMMIT" -> :commit
      "ROLLBACK" -> :rollback
      _ -> :execute
    end
  end
end
