use Mix.Config

# Using defaults the following can be shortened to:
# config :schemata,
#   cluster: [
#     keyspaces: [:schemata_test, :schemata_test_2]
#   ]
#
config :schemata,
  cluster: [
    username: 'cassandra',
    password: 'cassandra',
    seed_hosts: ['127.0.0.1'],
    keyspaces: [
      {:schemata_test, [
        strategy: :simple,
        factor: 1,
        clients: 1
      ]},
      :schemata_test_2
    ]
  ]

import_config "#{Mix.env}.exs"
