defmodule Schemata.Mixfile do
  use Mix.Project

  def project do
    [app: :schemata,
     version: "0.1.0",
     elixir: "~> 1.3",
     compilers: [:elixir, :erlang, :app],
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [description: 'Schemata library for interacting with Cassandra',
     applications: [:cqerl],
     env: [
       cassandra_hosts: [{'127.0.0.1', 9042}],
       cassandra_opts: []
     ]]
  end

  defp deps do
    [
      {:cqerl, github: "matehat/cqerl", branch: "master"}
    ]
  end
end
