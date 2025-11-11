defmodule Adbcex.Error do
  @moduledoc """
  Error raised when ADBC operations fail.
  """

  defexception [:message]

  @type t :: %__MODULE__{
          message: String.t()
        }
end
