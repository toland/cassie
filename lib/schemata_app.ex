defmodule SchemataApp do
  @moduledoc ""

  use Application
  use Schemata.CQErl
  require Logger

  @cluster_defaults [
    username: 'cassandra',
    password: 'cassandra',
    seed_hosts: ['127.0.0.1']
  ]

  @keyspace_defaults [
    strategy: :simple,
    factor: 1,
    clients: 1
  ]

  def start(_type, _args) do
    configure_cqerl

    _ =
      :schemata
      |> Application.fetch_env!(:clusters)
      |> configure_db

    Schemata.Supervisor.start_link()
  end

  defp configure_cqerl do
    # Set a few things that we depend on in cqerl
    Application.put_env(:cqerl, :maps, true, persistent: true)
    Application.put_env(:cqerl, :mode, :hash, persistent: true)
    Application.put_env(:cqerl, :text_uuids, true, persistent: true)
  end

  def configure_db(clusters) do
    for cluster <- clusters, do: configure_db_cluster(cluster)
  end

  def configure_db_cluster({name, cluster}) do
    :ok = Logger.info("Starting clients for cluster #{name}")
    cluster = Keyword.merge(@cluster_defaults, cluster)
    {hosts, opts} = extract_defaults(cluster)
    _ = start_default_client(name, hosts, opts)
    start_keyspace_clients(name, hosts, opts, cluster[:keyspaces])
  end

  defp extract_defaults(cluster) do
    opts = [
      auth: {:cqerl_auth_plain_handler, [
        {cluster[:username], cluster[:password]}
      ]}
    ]
    {cluster[:seed_hosts], opts}
  end

  defp start_default_client(name, hosts, opts) do
    :ok = Logger.info("Starting default client for cluster #{name}")
    CQErl.add_group(hosts, opts, 1)
  end

  defp start_keyspace_clients(name, hosts, opts, keyspaces) do
    for ks <- keyspaces, do: start_keyspace_client(name, hosts, opts, ks)
  end

  defp start_keyspace_client(cluster, hosts, opts, {name, config}) do
    :ok = Logger.info("Starting client for keyspace #{cluster}/#{name}")
    config = Keyword.merge(@keyspace_defaults, config)
    ensure_keyspace!(name, config)

    opts = Keyword.put(opts, :keyspace, name)
    CQErl.add_group(hosts, opts, config[:clients])
  end
  defp start_keyspace_client(cluster, hosts, opts, name),
    do: start_keyspace_client(cluster, hosts, opts, {name, []})

  defp ensure_keyspace!(name, config) do
    Schemata.create_keyspace name,
      strategy: config[:strategy],
      factor: config[:factor]
  end
end
