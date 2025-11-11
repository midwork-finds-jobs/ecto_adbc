defmodule EctoAdbc.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_adbc,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Ecto adapter for DuckDB using ADBC",
      package: package()
    ]
  end

  defp package do
    [
      maintainers: ["Your Name"],
      licenses: ["Apache-2.0"],
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
      {:decimal, "~> 1.6 or ~> 2.0"},
      {:jason, "~> 1.0"},
      {:explorer, "~> 0.11.1"},
      {:adbc, github: "elixir-explorer/adbc", ref: "8e44fc402627dd0c4c6077c24df27cf8a97654cd", override: true}
    ]
  end
end
