defmodule Schemata.Mixfile do
  use Mix.Project

  def project do
    [app: :schemata,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:logger, :cqerl],
     env: [
       cassandra_hosts: [{"localhost", 9042}],
       cassandra_opts: []
     ]]
  end

  defp deps do
    [
      {:cqerl, github: "matehat/cqerl", branch: "master"}
    ]
  end
end
