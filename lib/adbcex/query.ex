defmodule Adbcex.Query do
  @moduledoc """
  Query struct for ADBC operations.
  """

  @type t :: %__MODULE__{
          name: String.t(),
          statement: String.t(),
          ref: reference() | nil
        }

  defstruct name: "",
            statement: "",
            ref: nil
end

defimpl DBConnection.Query, for: Adbcex.Query do
  @moduledoc false

  def parse(query, _opts), do: query

  def describe(query, _opts), do: query

  def encode(_query, params, _opts), do: params

  def decode(_query, result, _opts), do: result
end

defimpl String.Chars, for: Adbcex.Query do
  def to_string(%Adbcex.Query{statement: statement}), do: statement
end
