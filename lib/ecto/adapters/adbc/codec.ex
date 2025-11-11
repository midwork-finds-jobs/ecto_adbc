defmodule Ecto.Adapters.Adbc.Codec do
  @moduledoc """
  Data encoding/decoding between Elixir and DuckDB via ADBC.
  """

  @doc """
  Decodes a boolean value from DuckDB storage.
  Handles: 0/1 integers, "0"/"1" strings, "FALSE"/"TRUE" strings, and booleans.
  """
  def bool_decode(0), do: {:ok, false}
  def bool_decode(1), do: {:ok, true}
  def bool_decode("0"), do: {:ok, false}
  def bool_decode("1"), do: {:ok, true}
  def bool_decode("FALSE"), do: {:ok, false}
  def bool_decode("TRUE"), do: {:ok, true}
  def bool_decode(false), do: {:ok, false}
  def bool_decode(true), do: {:ok, true}
  def bool_decode(nil), do: {:ok, nil}
  def bool_decode(other), do: {:ok, other}

  @doc """
  Decodes JSON from DuckDB storage using the configured JSON library.
  """
  def json_decode(nil), do: {:ok, nil}

  def json_decode(value) when is_binary(value) do
    json_library().decode(value)
  end

  def json_decode(value), do: {:ok, value}

  @doc """
  Decodes a float from DuckDB storage.
  """
  def float_decode(nil), do: {:ok, nil}
  def float_decode(value) when is_float(value), do: {:ok, value}
  def float_decode(%Decimal{} = value), do: {:ok, Decimal.to_float(value)}
  def float_decode(value) when is_integer(value), do: {:ok, value / 1}
  def float_decode(value), do: {:ok, value}

  @doc """
  Decodes a decimal from DuckDB storage.
  """
  def decimal_decode(nil), do: {:ok, nil}
  def decimal_decode(%Decimal{} = value), do: {:ok, value}
  def decimal_decode(value) when is_float(value), do: {:ok, Decimal.from_float(value)}
  def decimal_decode(value) when is_binary(value), do: Decimal.parse(value)
  def decimal_decode(value) when is_integer(value), do: {:ok, Decimal.new(value)}
  def decimal_decode(value), do: {:ok, value}

  @doc """
  Decodes a UTC datetime from ISO8601 format.
  """
  def utc_datetime_decode(nil), do: {:ok, nil}

  def utc_datetime_decode(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _} -> :error
    end
  end

  def utc_datetime_decode(%DateTime{} = value), do: {:ok, value}
  def utc_datetime_decode(_), do: :error

  @doc """
  Decodes a naive datetime from ISO8601 format.
  """
  def naive_datetime_decode(nil), do: {:ok, nil}

  def naive_datetime_decode(value) when is_binary(value) do
    case NaiveDateTime.from_iso8601(value) do
      {:ok, datetime} -> {:ok, datetime}
      {:error, _} -> :error
    end
  end

  def naive_datetime_decode(%NaiveDateTime{} = value), do: {:ok, value}
  def naive_datetime_decode(_), do: :error

  @doc """
  Decodes a date from ISO8601 format.
  """
  def date_decode(nil), do: {:ok, nil}

  def date_decode(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> {:ok, date}
      {:error, _} -> :error
    end
  end

  def date_decode(%Date{} = value), do: {:ok, value}
  def date_decode(_), do: :error

  @doc """
  Decodes a time from ISO8601 format.
  """
  def time_decode(nil), do: {:ok, nil}

  def time_decode(value) when is_binary(value) do
    case Time.from_iso8601(value) do
      {:ok, time} -> {:ok, time}
      {:error, _} -> :error
    end
  end

  def time_decode(%Time{} = value), do: {:ok, value}
  def time_decode(_), do: :error

  # Encoders

  @doc """
  Encodes JSON for DuckDB storage.
  """
  def json_encode(nil), do: {:ok, nil}

  def json_encode(value) do
    case json_library().encode(value) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _} = error -> error
    end
  end

  @doc """
  Encodes a blob for DuckDB storage.
  """
  def blob_encode(nil), do: {:ok, nil}
  def blob_encode(value), do: {:ok, value}

  @doc """
  Encodes a boolean for DuckDB storage (as 0 or 1).
  """
  def bool_encode(nil), do: {:ok, nil}
  def bool_encode(false), do: {:ok, 0}
  def bool_encode(true), do: {:ok, 1}

  @doc """
  Encodes a decimal for DuckDB storage.
  """
  def decimal_encode(nil), do: {:ok, nil}
  def decimal_encode(%Decimal{} = value), do: {:ok, Decimal.to_string(value)}

  @doc """
  Encodes a UTC datetime for DuckDB storage.
  """
  def utc_datetime_encode(nil), do: {:ok, nil}

  def utc_datetime_encode(%DateTime{} = value) do
    {:ok, DateTime.to_iso8601(value)}
  end

  @doc """
  Encodes a naive datetime for DuckDB storage.
  """
  def naive_datetime_encode(nil), do: {:ok, nil}

  def naive_datetime_encode(%NaiveDateTime{} = value) do
    {:ok, NaiveDateTime.to_iso8601(value)}
  end

  @doc """
  Encodes a date for DuckDB storage.
  """
  def date_encode(nil), do: {:ok, nil}
  def date_encode(%Date{} = value), do: {:ok, Date.to_iso8601(value)}

  @doc """
  Encodes a time for DuckDB storage.
  """
  def time_encode(nil), do: {:ok, nil}
  def time_encode(%Time{} = value), do: {:ok, Time.to_iso8601(value)}

  # Helpers

  defp json_library do
    Application.get_env(:ecto_adbc, :json_library, Jason)
  end
end
