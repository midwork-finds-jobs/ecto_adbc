defmodule Ecto.Adapters.Adbc.DataType do
  @moduledoc """
  Maps Ecto types to DuckDB column types via ADBC.
  """

  @doc """
  Receives an Ecto type and returns the corresponding DuckDB type.
  """
  def type_to_db(type, opts \\ [])

  # Numeric types
  def type_to_db(:id, _opts), do: "INTEGER"
  def type_to_db(:serial, _opts), do: "INTEGER"
  def type_to_db(:bigserial, _opts), do: "BIGINT"
  def type_to_db(:integer, _opts), do: "INTEGER"
  def type_to_db(:bigint, _opts), do: "BIGINT"
  def type_to_db(:float, _opts), do: "NUMERIC"

  # Boolean (stored as 0/1 integers)
  def type_to_db(:boolean, _opts), do: "INTEGER"

  # String types
  def type_to_db(:string, _opts), do: "TEXT"
  def type_to_db(:text, _opts), do: "TEXT"
  def type_to_db(:binary, _opts), do: "BLOB"

  # Date/Time types (stored as TEXT in ISO8601 format)
  def type_to_db(:date, _opts), do: "TEXT"
  def type_to_db(:time, _opts), do: "TEXT"
  def type_to_db(:time_usec, _opts), do: "TEXT"
  def type_to_db(:naive_datetime, _opts), do: "TEXT"
  def type_to_db(:naive_datetime_usec, _opts), do: "TEXT"
  def type_to_db(:utc_datetime, _opts), do: "TEXT"
  def type_to_db(:utc_datetime_usec, _opts), do: "TEXT"

  # Decimal
  def type_to_db(:decimal, opts) do
    precision = Keyword.get(opts, :precision, 10)
    scale = Keyword.get(opts, :scale, 0)
    "DECIMAL(#{precision},#{scale})"
  end

  # UUID and Binary ID (configurable storage)
  def type_to_db(:binary_id, _opts) do
    case Application.get_env(:ecto_adbc, :binary_id_type, :string) do
      :string -> "TEXT"
      :binary -> "BLOB"
    end
  end

  def type_to_db(:uuid, _opts) do
    case Application.get_env(:ecto_adbc, :uuid_type, :string) do
      :string -> "TEXT"
      :binary -> "BLOB"
    end
  end

  # Map/JSON (configurable storage)
  def type_to_db(:map, _opts) do
    case Application.get_env(:ecto_adbc, :map_type, :string) do
      :string -> "TEXT"
      :binary -> "BLOB"
    end
  end

  # Array (configurable storage)
  def type_to_db({:array, _}, _opts) do
    case Application.get_env(:ecto_adbc, :array_type, :string) do
      :string -> "TEXT"
      :binary -> "BLOB"
    end
  end

  # Geometry (requires DuckDB spatial extension)
  # Must run "INSTALL spatial" and "LOAD spatial" before using
  def type_to_db(:geometry, _opts), do: "GEOMETRY"

  # Fallback
  def type_to_db(other, _opts), do: raise("Unsupported Ecto type: #{inspect(other)}")
end
