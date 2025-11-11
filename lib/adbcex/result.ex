defmodule Adbcex.Result do
  @moduledoc """
  Result struct for ADBC query responses.
  """

  @type t :: %__MODULE__{
          command: atom(),
          columns: [String.t()] | nil,
          rows: [[term()]] | nil,
          num_rows: non_neg_integer()
        }

  defstruct command: :execute,
            columns: nil,
            rows: nil,
            num_rows: 0
end
