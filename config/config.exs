use Mix.Config

# Using defaults the following can be shortened to:
# config :schemata,
#   clusters: [
#     default: [
#       keyspaces: [:schemata_test]
#     ]
#   ]
#
config :schemata,
  clusters: [
    default: [
      username: 'cassandra',
      password: 'cassandra',
      seed_hosts: ['127.0.0.1'],
      keyspaces: [
        schemata_test: [
          strategy: :simple,
          factor: 1,
          clients: 1
        ]
      ]
    ]
  ]

import_config "#{Mix.env}.exs"
