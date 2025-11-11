defmodule EctoAdbcTest do
  use ExUnit.Case
  doctest EctoAdbc

  test "greets the world" do
    assert EctoAdbc.hello() == :world
  end
end
