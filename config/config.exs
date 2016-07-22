use Mix.Config

config :cqerl,
  text_uuids: true

config :schemata,
  clusters: [
    [
      username: 'cassandra',
      password: 'cassandra',
      seed_hosts: ['127.0.0.1'],
      keyspaces: [
        schemata_test: [
          strategy: :simple,
          factor: 1,
          clients: 1
        ],
        schemata: [
          strategy: :simple,
          factor: 1,
          clients: 1
        ]
      ]
    ]
  ]

import_config "#{Mix.env}.exs"
