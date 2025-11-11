defmodule EctoAdbc do
  @moduledoc """
  Ecto adapter for DuckDB using ADBC.
  """

  def child_spec(opts) do
    DBConnection.child_spec(Adbcex.Connection, opts)
  end
end
