use Mix.Config

config :cqerl,
  text_uuids: true,
  client_groups: [
    client_group: [
      hosts: ['127.0.0.1'],
      opts: [
        auth: {:cqerl_auth_plain_handler, [{'cassandra', 'cassandra'}]}
      ],
      clients_per_server: 1
    ],
    client_group: [
      hosts: ['127.0.0.1'],
      opts: [
        keyspace: :schemata_test,
        auth: {:cqerl_auth_plain_handler, [{'cassandra', 'cassandra'}]}
      ],
      clients_per_server: 1
    ],
    client_group: [
      hosts: ['127.0.0.1'],
      opts: [
        keyspace: :schemata,
        auth: {:cqerl_auth_plain_handler, [{'cassandra', 'cassandra'}]}
      ],
      clients_per_server: 1
    ]
  ]
