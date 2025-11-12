defmodule EctoAdbc.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_adbc,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Ecto adapter for ADBC to use DuckDB",
      package: package()
    ]
  end

  defp package do
    [
      maintainers: ["Onni Hakala"],
      licenses: ["MIT"],
      links: %{}
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Adbcex.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.13"},
      {:ecto, "~> 3.13"},
      {:db_connection, "~> 2.0"},
      {:adbc, "~> 0.8.0"},
      {:explorer, "~> 0.11.1"}
    ]
  end
end
