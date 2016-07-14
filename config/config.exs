use Mix.Config

config :cqerl,
  client_groups: [
    client_group: [
      name: :no_ks,
      hosts: ['localhost'],
      opts: [auth: {:cqerl_auth_plain_handler, [{"cassandra", "cassandra"}]}],
      clients_per_server: 1
    ]
  ]
