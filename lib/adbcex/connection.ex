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
    require Logger

    Logger.info("Adbcex.Connection.connect - RAW OPTS: #{inspect(opts)}")

    database = Keyword.get(opts, :database, ":memory:")
    driver = Keyword.get(opts, :driver, :duckdb)
    version = Keyword.get(opts, :version, Ecto.Adapters.Adbc.default_duckdb_version())

    Logger.info("Adbcex.Connection.connect - database: #{inspect(database)}")
    Logger.info("Adbcex.Connection.connect - driver: #{inspect(driver)}")
    Logger.info("Adbcex.Connection.connect - version: #{inspect(version)}")
    Logger.info("Adbcex.Connection.connect - driver is_binary?: #{inspect(is_binary(driver))}")

    # If driver is an atom, get the path to the installed driver (installing if needed)
    # If driver is already a string (path), use it directly
    driver_path = if is_binary(driver) do
      driver
    else
      ensure_driver_installed(driver, version)
    end

    Logger.info("Adbcex.Connection.connect - driver_path: #{inspect(driver_path)}")

    # Start ADBC Database with driver path
    db_opts = [driver: driver_path]
    db_opts = if database != ":memory:", do: Keyword.put(db_opts, :path, database), else: db_opts

    Logger.info("Adbcex.Connection.connect - db_opts being passed to Adbc.Database.start_link: #{inspect(db_opts)}")

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

  defp ensure_driver_installed(driver, version) do
    # Install driver to a writable directory and return its path
    # Use a lock file to prevent race conditions when multiple processes try to install
    require Logger

    # Get writable installation directory
    install_dir = get_install_dir()
    Logger.info("Using installation directory: #{inspect(install_dir)}")

    # Get platform info
    {os, arch} = get_platform()

    # Determine driver filename based on platform
    driver_filename = case os do
      "windows" -> "adbc_driver_#{driver}.dll"
      "osx" -> "libadbc_driver_#{driver}.dylib"
      _ -> "libadbc_driver_#{driver}.so"
    end

    driver_path = Path.join(install_dir, driver_filename)

    # Check if driver already exists
    if File.exists?(driver_path) do
      Logger.info("Driver already installed at #{driver_path}")
      driver_path
    else
      # Use lock file to prevent concurrent installations
      lock_file = Path.join(install_dir, ".#{driver}-#{version}.lock")
      File.mkdir_p!(install_dir)

      # Try to acquire lock
      case File.open(lock_file, [:write, :exclusive]) do
        {:ok, lock_fd} ->
          try do
            # Double-check after acquiring lock
            if File.exists?(driver_path) do
              Logger.info("Driver already installed (found after lock acquisition)")
              driver_path
            else
              do_install_driver(driver, version, os, arch, install_dir, driver_path, driver_filename)
            end
          after
            File.close(lock_fd)
            File.rm(lock_file)
          end

        {:error, :eexist} ->
          # Another process is installing, wait for it
          Logger.info("Waiting for another process to finish installing driver...")
          wait_for_driver(driver_path, 30_000)
      end
    end
  end

  defp do_install_driver(driver, version, os, arch, install_dir, driver_path, driver_filename) do
    require Logger

    Logger.info("Installing #{driver} driver version #{version} for #{os}-#{arch}...")

    # Download the driver archive
    url = build_driver_url(driver, version, os, arch)
    Logger.info("Downloading from: #{url}")

    # Use Adbc's download mechanism but to our cache directory
    cache_dir = get_cache_dir()
    File.mkdir_p!(cache_dir)

    cache_file = Path.join(cache_dir, "#{driver}-#{version}-#{os}-#{arch}.tar.gz")

    unless File.exists?(cache_file) do
      Logger.info("Downloading to cache: #{cache_file}")
      download_file(url, cache_file)
    else
      Logger.info("Using cached file: #{cache_file}")
    end

    # Extract the driver to installation directory
    extract_driver(cache_file, install_dir, driver_filename)

    Logger.info("Driver installed successfully at #{driver_path}")
    driver_path
  end

  defp wait_for_driver(driver_path, timeout) do
    require Logger
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      if File.exists?(driver_path) do
        {:ok, driver_path}
      else
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(100)
          :continue
        else
          {:error, :timeout}
        end
      end
    end)
    |> Enum.find(fn
      {:ok, _} -> true
      {:error, _} -> true
      :continue -> false
    end)
    |> case do
      {:ok, path} ->
        Logger.info("Driver installation completed by another process")
        path

      {:error, :timeout} ->
        raise "Timeout waiting for driver installation"
    end
  end

  defp get_install_dir do
    # Use ADBC_CACHE_DIR if set, otherwise fall back to HOME/.cache/adbc
    base_dir = System.get_env("ADBC_CACHE_DIR") ||
               Path.join(System.get_env("HOME") || "/tmp", ".cache/adbc")
    Path.join(base_dir, "drivers")
  end

  defp get_cache_dir do
    # Separate cache directory for downloaded archives
    base_dir = System.get_env("ADBC_CACHE_DIR") ||
               Path.join(System.get_env("HOME") || "/tmp", ".cache/adbc")
    Path.join(base_dir, "downloads")
  end

  defp download_file(url, dest_path) do
    require Logger

    # Use :httpc to download the file
    :inets.start()
    :ssl.start()

    url_charlist = String.to_charlist(url)

    case :httpc.request(:get, {url_charlist, []}, [{:timeout, 60000}], [body_format: :binary]) do
      {:ok, {{_, 200, _}, _headers, body}} ->
        File.write!(dest_path, body)
        Logger.info("Downloaded #{byte_size(body)} bytes")
        :ok

      {:ok, {{_, status, _}, _headers, _body}} ->
        raise "HTTP request failed with status #{status}"

      {:error, reason} ->
        raise "HTTP request failed: #{inspect(reason)}"
    end
  end

  defp extract_driver(archive_file, dest_dir, driver_filename) do
    require Logger

    # Extract tar.gz archive using system tar command
    # This is more reliable than using Erlang's :erl_tar
    System.cmd("tar", ["-xzf", archive_file, "-C", dest_dir],
      stderr_to_stdout: true)
    |> case do
      {output, 0} ->
        Logger.info("Extracted archive: #{output}")

        # List all extracted files
        {:ok, files} = File.ls(dest_dir)
        Logger.info("Extracted files: #{inspect(files)}")

        # Find the driver library in extracted files
        driver_file = Enum.find(files, fn file ->
          String.ends_with?(file, driver_filename) or
          String.ends_with?(file, ".so") or
          String.ends_with?(file, ".dylib") or
          String.ends_with?(file, ".dll")
        end)

        if driver_file do
          # Move/rename to expected location if needed
          extracted_path = Path.join(dest_dir, driver_file)
          expected_path = Path.join(dest_dir, driver_filename)

          unless extracted_path == expected_path do
            File.rename!(extracted_path, expected_path)
            Logger.info("Renamed #{driver_file} to #{driver_filename}")
          end

          :ok
        else
          raise "Could not find driver library in extracted files: #{inspect(files)}"
        end

      {output, exit_code} ->
        raise "Failed to extract archive (exit code #{exit_code}): #{output}"
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
    # ADBC drivers are released by Apache Arrow ADBC project
    # The DuckDB ADBC driver version corresponds to ADBC version, not DuckDB version
    # For now, we'll use a mapping or default to a known working version
    # TODO: Make this configurable or fetch the latest ADBC release version

    # Map DuckDB version to ADBC version (this is approximate)
    # For DuckDB 1.4.x, we should use ADBC 0.8.0 or later
    adbc_version = "0.8.0"

    # Map arch names to ADBC release naming
    {os_name, arch_name} = case {os, arch} do
      {"osx", "aarch64"} -> {"macos", "arm64"}
      {"osx", "amd64"} -> {"macos", "x86_64"}
      {"linux", "aarch64"} -> {"linux", "arm64"}
      {"linux", "amd64"} -> {"linux", "x86_64"}
      {"windows", "amd64"} -> {"windows", "amd64"}
      _ -> raise "Unsupported platform: #{os}-#{arch}"
    end

    # Apache Arrow ADBC releases the drivers as tar.gz files
    "https://github.com/apache/arrow-adbc/releases/download/apache-arrow-adbc-#{adbc_version}/adbc_driver_duckdb-#{adbc_version}-#{os_name}-#{arch_name}.tar.gz"
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
