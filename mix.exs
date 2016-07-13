defmodule Schemata.Mixfile do
  use Mix.Project

  def project do
    [app: :schemata,
     version: "0.1.0",
     elixir: "~> 1.3",
     compilers: [:elixir, :erlang, :app],
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     dialyzer: [
       plt_add_deps: :transitive,
       plt_add_apps: [:ssl],
       flags: [
         "--fullpath",
         "-Wunmatched_returns",
         "-Werror_handling",
         "-Wrace_conditions",
         "-Wunderspecs",
         "-Wunknown"
       ]
     ],
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
      {:dialyxir, "~> 0.3", only: :dev},
      {:dogma,    "~> 0.1", only: :dev},
      {:credo,    "~> 0.4", only: :dev},
      {:cqerl, github: "hippware/cqerl", branch: "working-2.0", manager: :rebar3},
      # erlando's app file is b0rked so we need to override the dep here.
      {:erlando, ~r//, github: "rabbitmq/erlando", branch: "master", override: true}
    ]
  end
end
