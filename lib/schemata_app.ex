defmodule SchemataApp do
  @moduledoc ""

  use Application
  use Schemata.CQErl
  require Logger

  def start(_type, _args) do
    :schemata
    |> Application.fetch_env!(:clusters)
    |> configure_db

    Schemata.Supervisor.start_link()
  end

  defp configure_db(clusters) do
    for cluster <- clusters, do: configure_db_cluster(cluster)
  end

  defp configure_db_cluster(cluster) do
    {hosts, opts} = extract_defaults(cluster)
    _ = start_default_client(hosts, opts)
    start_keyspace_clients(hosts, opts, cluster[:keyspaces])
  end

  defp extract_defaults(cluster) do
    opts = [
      auth: {:cqerl_auth_plain_handler, [
        {cluster[:username], cluster[:password]}
      ]}
    ]
    {cluster[:seed_hosts], opts}
  end

  defp start_default_client(hosts, opts) do
    Logger.info("Starting default client for hosts #{inspect(hosts)}")
    CQErl.add_group(hosts, opts, 1)
  end

  defp start_keyspace_clients(hosts, opts, keyspaces) do
    for ks <- keyspaces, do: start_keyspace_client(hosts, opts, ks)
  end

  defp start_keyspace_client(hosts, opts, {name, config}) do
    Logger.info("Starting client for keyspace #{name}")
    ensure_keyspace!(name, config)

    opts = Keyword.put(opts, :keyspace, name)
    CQErl.add_group(hosts, opts, config[:clients])
  end

  defp ensure_keyspace!(name, config) do
    Schemata.create_keyspace name,
      strategy: config[:strategy],
      factor: config[:factor]
  end
end
