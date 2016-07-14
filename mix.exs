defmodule Schemata.Mixfile do
  use Mix.Project

  def project do
    [app: :schemata,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     test_coverage: [tool: Coverex.Task, ignore_modules: [:schemata]],
     preferred_cli_env: [espec: :test, spec: :test],
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
     aliases: aliases,
     deps: deps]
  end

  def application do
    [description: 'Elixir library for interacting with Cassandra',
     applications: [:cqerl],
     env: []]
  end

  defp aliases do
    [spec: "espec --cover"]
  end

  defp deps do
    [
      {:dialyxir, "~> 0.3", only: :dev},
      {:dogma,    "~> 0.1", only: :dev},
      {:credo,    "~> 0.4", only: :dev},
      {:ex_guard, "~> 1.1", only: :dev},
      {:espec,    "~> 0.8", only: :test},
      {:coverex,  "~> 1.4", only: :test},

      {:cqerl, github: "hippware/cqerl", branch: "working-2.0", manager: :rebar3},

      # erlando's app file is b0rked so we need to override the dep here.
      {:erlando, ~r//, github: "rabbitmq/erlando", branch: "master", override: true}
    ]
  end
end
